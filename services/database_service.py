"""
Database service for handling PostgreSQL operations.

This module provides services for database operations including
report storage, retrieval, and metadata management.
"""

import json
import uuid
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
import psycopg2
from psycopg2.extras import Json
from psycopg2.extensions import connection, cursor

from utils.exceptions import DatabaseError
from models.log_metadata import AnalysisReport
from config.settings import settings


logger = logging.getLogger(__name__)


class DatabaseService:
    """Service for database operations."""
    
    def __init__(self):
        self.db_config = settings.database
    
    def get_connection(self) -> connection:
        """Get a database connection."""
        try:
            return psycopg2.connect(
                dbname=self.db_config.dbname,
                user=self.db_config.user,
                password=self.db_config.password,
                host=self.db_config.host,
                port=self.db_config.port
            )
        except Exception as e:
            raise DatabaseError(f"Failed to connect to database: {e}")
    
    def store_report(self, report: AnalysisReport) -> str:
        """
        Store an analysis report in the database.
        
        Args:
            report: AnalysisReport to store
            
        Returns:
            Report ID (UUID string)
            
        Raises:
            DatabaseError: If storage fails
        """
        report_id = str(uuid.uuid4())
        
        try:
            logger.info(f"Attempting to store report with ID: {report_id}")
            logger.info(f"Database config: host={self.db_config.host}, port={self.db_config.port}, dbname={self.db_config.dbname}, user={self.db_config.user}")
            
            with self.get_connection() as conn:
                logger.info("Database connection established successfully")
                
                with conn.cursor() as cur:
                    logger.info("Executing INSERT statement...")
                    cur.execute(
                        """
                        INSERT INTO public.log_analyzer_reports 
                        (id, support_bundle_name, json_report, created_at)
                        VALUES (%s, %s, %s, NOW())
                        """,
                        (report_id, report.support_bundle_name, Json(report.to_dict()))
                    )
                    conn.commit()
                    logger.info("INSERT statement executed and committed successfully")
            
            logger.info(f"Report stored with ID: {report_id}")
            return report_id
            
        except Exception as e:
            logger.error(f"Failed to store report: {e}")
            logger.error(f"Exception type: {type(e).__name__}")
            raise DatabaseError(f"Failed to store report: {e}")
    
    def get_report(self, report_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a report from the database.
        
        Args:
            report_id: Report ID (UUID string)
            
        Returns:
            Report data dictionary or None if not found
            
        Raises:
            DatabaseError: If retrieval fails
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT json_report FROM public.log_analyzer_reports 
                        WHERE id::text = %s
                        """,
                        (report_id,)
                    )
                    row = cur.fetchone()
                    
                    if row:
                        return row[0]
                    else:
                        return None
                        
        except Exception as e:
            raise DatabaseError(f"Failed to retrieve report: {e}")
    
    def get_reports_list(
        self, 
        page: int = 1, 
        per_page: int = 10,
        search_query: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get a paginated list of reports.
        
        Args:
            page: Page number (1-based)
            per_page: Number of reports per page
            search_query: Optional search query
            
        Returns:
            Dictionary with reports list and pagination info
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Build query based on search
                    if search_query:
                        where_clause = """
                            WHERE r.id::text ILIKE %s
                               OR r.support_bundle_name ILIKE %s
                               OR h.cluster_name ILIKE %s
                               OR h.organization ILIKE %s
                               OR h.case_id::text ILIKE %s
                        """
                        search_pattern = f"%{search_query}%"
                        params = [search_pattern] * 5
                    else:
                        where_clause = ""
                        params = []
                    
                    # Count total
                    count_sql = f"""
                        SELECT COUNT(*)
                        FROM public.log_analyzer_reports r
                        LEFT JOIN public.support_bundle_header h ON r.support_bundle_name = h.support_bundle
                        {where_clause}
                    """
                    cur.execute(count_sql, params)
                    total_reports = cur.fetchone()[0]
                    
                    # Calculate pagination
                    total_pages = (total_reports + per_page - 1) // per_page if total_reports else 1
                    offset = (page - 1) * per_page
                    
                    # Get reports
                    reports_sql = f"""
                        SELECT r.id, r.support_bundle_name, h.cluster_name, h.organization, h.case_id, r.created_at
                        FROM public.log_analyzer_reports r
                        LEFT JOIN public.support_bundle_header h ON r.support_bundle_name = h.support_bundle
                        {where_clause}
                        ORDER BY r.created_at DESC 
                        LIMIT %s OFFSET %s
                    """
                    cur.execute(reports_sql, params + [per_page, offset])
                    
                    reports = [
                        {
                            'id': str(row[0]),
                            'support_bundle_name': row[1],
                            'universe_name': row[2] or '',
                            'organization_name': row[3] or '',
                            'case_id': row[4],
                            'created_at': row[5].strftime('%Y-%m-%d %H:%M')
                        }
                        for row in cur.fetchall()
                    ]
                    
                    return {
                        'reports': reports,
                        'page': page,
                        'total_pages': total_pages,
                        'total_reports': total_reports
                    }
                    
        except Exception as e:
            raise DatabaseError(f"Failed to retrieve reports list: {e}")
    
    def get_related_reports(self, report_id: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get related reports (same cluster, same organization).
        
        Args:
            report_id: Report ID to find related reports for
            
        Returns:
            Dictionary with 'same_cluster' and 'same_org' lists
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Get support bundle name for this report
                    cur.execute(
                        "SELECT support_bundle_name FROM public.log_analyzer_reports WHERE id::text = %s",
                        (report_id,)
                    )
                    row = cur.fetchone()
                    if not row:
                        return {'same_cluster': [], 'same_org': []}
                    
                    support_bundle_name = row[0]
                    
                    # Get cluster and organization info
                    cur.execute(
                        """
                        SELECT h.cluster_uuid, h.organization
                        FROM public.support_bundle_header h
                        WHERE h.support_bundle = %s
                        """,
                        (support_bundle_name,)
                    )
                    row = cur.fetchone()
                    if not row:
                        return {'same_cluster': [], 'same_org': []}
                    
                    cluster_uuid, organization = row
                    
                    # Get same cluster reports
                    cur.execute(
                        """
                        SELECT r.id, r.support_bundle_name, h.cluster_name, h.organization, h.cluster_uuid, h.case_id, r.created_at
                        FROM public.log_analyzer_reports r
                        JOIN public.support_bundle_header h ON r.support_bundle_name = h.support_bundle
                        WHERE h.cluster_uuid = %s AND r.id::text != %s
                        ORDER BY r.created_at DESC LIMIT 20
                        """,
                        (str(cluster_uuid), report_id)
                    )
                    
                    same_cluster = [
                        {
                            'id': str(r[0]),
                            'support_bundle_name': r[1],
                            'cluster_name': r[2],
                            'organization': r[3],
                            'cluster_uuid': str(r[4]),
                            'case_id': r[5],
                            'created_at': r[6].strftime('%Y-%m-%d %H:%M')
                        }
                        for r in cur.fetchall()
                    ]
                    
                    # Get same organization reports (not same cluster)
                    cur.execute(
                        """
                        SELECT r.id, r.support_bundle_name, h.cluster_name, h.organization, h.cluster_uuid, h.case_id, r.created_at
                        FROM public.log_analyzer_reports r
                        JOIN public.support_bundle_header h ON r.support_bundle_name = h.support_bundle
                        WHERE h.organization = %s AND h.cluster_uuid != %s AND r.id::text != %s
                        ORDER BY r.created_at DESC LIMIT 20
                        """,
                        (organization, str(cluster_uuid), report_id)
                    )
                    
                    same_org = [
                        {
                            'id': str(r[0]),
                            'support_bundle_name': r[1],
                            'cluster_name': r[2],
                            'organization': r[3],
                            'cluster_uuid': str(r[4]),
                            'case_id': r[5],
                            'created_at': r[6].strftime('%Y-%m-%d %H:%M')
                        }
                        for r in cur.fetchall()
                    ]
                    
                    return {'same_cluster': same_cluster, 'same_org': same_org}
                    
        except Exception as e:
            raise DatabaseError(f"Failed to retrieve related reports: {e}")
    
    def get_gflags(self, report_id: str) -> Dict[str, Any]:
        """
        Get GFlags for a report.
        
        Args:
            report_id: Report ID
            
        Returns:
            Dictionary of GFlags by server type
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Get support bundle name
                    cur.execute(
                        "SELECT support_bundle_name FROM public.log_analyzer_reports WHERE id::text = %s",
                        (report_id,)
                    )
                    row = cur.fetchone()
                    if not row:
                        return {}
                    
                    support_bundle_name = row[0]
                    
                    # Get GFlags
                    cur.execute(
                        """
                        SELECT server_type, gflag, value
                        FROM public.support_bundle_gflags
                        WHERE support_bundle = %s
                        """,
                        (support_bundle_name,)
                    )
                    
                    gflags = {}
                    for server_type, gflag, value in cur.fetchall():
                        if server_type not in gflags:
                            gflags[server_type] = {}
                        gflags[server_type][gflag] = value
                    
                    return gflags
                    
        except Exception as e:
            raise DatabaseError(f"Failed to retrieve GFlags: {e}")
    
    def get_node_info(self, report_id: str) -> Dict[str, Any]:
        """
        Get node information for a report.
        
        Args:
            report_id: Report ID
            
        Returns:
            Dictionary with node information
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Get support bundle name
                    cur.execute(
                        "SELECT support_bundle_name FROM public.log_analyzer_reports WHERE id::text = %s",
                        (report_id,)
                    )
                    row = cur.fetchone()
                    if not row:
                        return {'nodes': []}
                    
                    support_bundle_name = row[0]
                    
                    # Get node info
                    cur.execute(
                        """
                        SELECT node_name, state, is_master, is_tserver, 
                               cloud || '.' || region || '.' || az as placement, 
                               num_cores, mem_size_gb, volume_size_gb
                        FROM public.view_support_bundle_yba_metadata_cluster_summary
                        WHERE support_bundle = %s
                        """,
                        (support_bundle_name,)
                    )
                    
                    nodes = []
                    for r in cur.fetchall():
                        nodes.append({
                            'node_name': r[0],
                            'state': r[1],
                            'is_master': r[2],
                            'is_tserver': r[3],
                            'placement': r[4],
                            'num_cores': r[5],
                            'mem_size_gb': float(r[6]) if r[6] is not None else None,
                            'volume_size_gb': float(r[7]) if r[7] is not None else None
                        })
                    
                    return {'nodes': nodes}
                    
        except Exception as e:
            raise DatabaseError(f"Failed to retrieve node info: {e}")
    
    def check_report_exists(self, support_bundle_name: str) -> Optional[str]:
        """
        Check if a report already exists for a support bundle.
        
        Args:
            support_bundle_name: Name of the support bundle
            
        Returns:
            Report ID if exists, None otherwise
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id FROM public.log_analyzer_reports 
                        WHERE support_bundle_name = %s
                        ORDER BY created_at DESC LIMIT 1
                        """,
                        (support_bundle_name,)
                    )
                    row = cur.fetchone()
                    return str(row[0]) if row else None
                    
        except Exception as e:
            logger.warning(f"Failed to check report existence: {e}")
            return None
    
    def delete_report(self, report_id: str) -> bool:
        """
        Delete a report from the database by its UUID.
        
        Args:
            report_id: Report ID (UUID string)
            
        Returns:
            True if a report was deleted, False otherwise
            
        Raises:
            DatabaseError: If deletion fails
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        DELETE FROM public.log_analyzer_reports WHERE id::text = %s
                        """,
                        (report_id,)
                    )
                    deleted = cur.rowcount
                    conn.commit()
                    return deleted > 0
        except Exception as e:
            raise DatabaseError(f"Failed to delete report: {e}")