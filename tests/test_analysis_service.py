"""
Tests for the Analysis Service.

This module contains unit tests for the main analysis service functionality.
"""

import pytest
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from models.log_metadata import AnalysisConfig, AnalysisReport
from services.analysis_service import AnalysisService
from utils.exceptions import AnalysisError, ValidationError


class TestAnalysisService:
    """Test cases for AnalysisService."""
    
    @pytest.fixture
    def analysis_service(self):
        """Create an AnalysisService instance for testing."""
        return AnalysisService()
    
    @pytest.fixture
    def sample_analysis_config(self):
        """Create a sample analysis configuration."""
        start_time = datetime.now() - timedelta(days=1)
        end_time = datetime.now()
        
        return AnalysisConfig(
            start_time=start_time,
            end_time=end_time,
            parallel_threads=2,
            histogram_mode=None,
            node_filter=None,
            log_type_filter=None
        )
    
    def test_analysis_config_validation(self):
        """Test AnalysisConfig validation."""
        # Valid configuration
        start_time = datetime.now() - timedelta(days=1)
        end_time = datetime.now()
        
        config = AnalysisConfig(
            start_time=start_time,
            end_time=end_time,
            parallel_threads=5
        )
        config.validate()  # Should not raise
        
        # Invalid: start time after end time
        with pytest.raises(ValueError, match="Start time cannot be after end time"):
            config = AnalysisConfig(
                start_time=end_time,
                end_time=start_time,
                parallel_threads=5
            )
            config.validate()
        
        # Invalid: too few threads
        with pytest.raises(ValueError, match="Parallel threads must be at least 1"):
            config = AnalysisConfig(
                start_time=start_time,
                end_time=end_time,
                parallel_threads=0
            )
            config.validate()
        
        # Invalid: too many threads
        with pytest.raises(ValueError, match="Parallel threads cannot exceed 20"):
            config = AnalysisConfig(
                start_time=start_time,
                end_time=end_time,
                parallel_threads=25
            )
            config.validate()
    
    @patch('services.analysis_service.FileProcessor')
    @patch('services.analysis_service.PatternMatcher')
    def test_analyze_support_bundle_success(
        self, 
        mock_pattern_matcher, 
        mock_file_processor,
        analysis_service,
        sample_analysis_config
    ):
        """Test successful support bundle analysis."""
        # Mock file processor
        mock_processor = Mock()
        mock_file_processor.return_value = mock_processor
        
        # Mock extracted directory
        mock_extracted_dir = Path("/tmp/extracted")
        mock_processor.extract_support_bundle.return_value = mock_extracted_dir
        
        # Mock support bundle info
        mock_support_bundle_info = Mock()
        mock_support_bundle_info.name = "test_bundle"
        mock_support_bundle_info.log_files_metadata = {}
        
        # Mock analysis results
        mock_results = {}
        
        # Mock the private methods
        with patch.object(analysis_service, '_build_support_bundle_info') as mock_build:
            with patch.object(analysis_service, '_analyze_logs') as mock_analyze:
                with patch.object(analysis_service, '_generate_report') as mock_generate:
                    mock_build.return_value = mock_support_bundle_info
                    mock_analyze.return_value = mock_results
                    
                    mock_report = Mock(spec=AnalysisReport)
                    mock_generate.return_value = mock_report
                    
                    # Test the method
                    bundle_path = Path("/tmp/test_bundle.tar.gz")
                    result = analysis_service.analyze_support_bundle(
                        bundle_path=bundle_path,
                        analysis_config=sample_analysis_config,
                        skip_extraction=False
                    )
                    
                    # Verify calls
                    mock_processor.extract_support_bundle.assert_called_once_with(bundle_path)
                    mock_build.assert_called_once_with(mock_extracted_dir, "test_bundle.tar.gz")
                    mock_analyze.assert_called_once_with(mock_support_bundle_info, sample_analysis_config)
                    mock_generate.assert_called_once_with(mock_support_bundle_info, mock_results, sample_analysis_config)
                    
                    assert result == mock_report
    
    @patch('services.analysis_service.FileProcessor')
    def test_analyze_support_bundle_extraction_error(
        self, 
        mock_file_processor,
        analysis_service,
        sample_analysis_config
    ):
        """Test support bundle analysis with extraction error."""
        # Mock file processor to raise an error
        mock_processor = Mock()
        mock_file_processor.return_value = mock_processor
        mock_processor.extract_support_bundle.side_effect = Exception("Extraction failed")
        
        # Test the method
        bundle_path = Path("/tmp/test_bundle.tar.gz")
        
        with pytest.raises(AnalysisError, match="Analysis failed"):
            analysis_service.analyze_support_bundle(
                bundle_path=bundle_path,
                analysis_config=sample_analysis_config,
                skip_extraction=False
            )
    
    def test_filter_files_by_time(self, analysis_service):
        """Test file filtering by time range."""
        from datetime import datetime
        
        # Create sample file metadata
        files_metadata = {
            "/path/file1.log": Mock(
                start_time=datetime(2023, 12, 31, 10, 0),
                end_time=datetime(2023, 12, 31, 12, 0)
            ),
            "/path/file2.log": Mock(
                start_time=datetime(2023, 12, 31, 14, 0),
                end_time=datetime(2023, 12, 31, 16, 0)
            ),
            "/path/file3.log": Mock(
                start_time=datetime(2023, 12, 31, 8, 0),
                end_time=datetime(2023, 12, 31, 9, 0)
            )
        }
        
        # Test time range that overlaps with files 1 and 2
        start_time = datetime(2023, 12, 31, 11, 0)
        end_time = datetime(2023, 12, 31, 15, 0)
        
        filtered_files = analysis_service._filter_files_by_time(
            files_metadata, start_time, end_time
        )
        
        # Should return files 1 and 2 (file 3 is outside the range)
        assert len(filtered_files) == 2
        assert "/path/file1.log" in filtered_files
        assert "/path/file2.log" in filtered_files
        assert "/path/file3.log" not in filtered_files
    
    def test_save_and_load_report(self, analysis_service, tmp_path):
        """Test saving and loading reports."""
        # Create a sample report
        report = AnalysisReport(
            support_bundle_name="test_bundle",
            nodes={},
            warnings=[],
            analysis_config={}
        )
        
        # Save report
        output_path = tmp_path / "test_report.json"
        analysis_service.save_report(report, output_path)
        
        # Verify file was created
        assert output_path.exists()
        
        # Load report
        loaded_report = analysis_service.load_report(output_path)
        
        # Verify loaded report has correct structure
        assert loaded_report.support_bundle_name == "test_bundle"
        assert isinstance(loaded_report.nodes, dict)
        assert isinstance(loaded_report.warnings, list)
    
    def test_save_report_error(self, analysis_service):
        """Test saving report with invalid path."""
        report = AnalysisReport(
            support_bundle_name="test_bundle",
            nodes={},
            warnings=[],
            analysis_config={}
        )
        
        # Try to save to a directory that doesn't exist
        invalid_path = Path("/nonexistent/directory/report.json")
        
        with pytest.raises(AnalysisError, match="Failed to save report"):
            analysis_service.save_report(report, invalid_path)


class TestAnalysisConfig:
    """Test cases for AnalysisConfig."""
    
    def test_analysis_config_creation(self):
        """Test AnalysisConfig creation with valid parameters."""
        start_time = datetime.now() - timedelta(days=1)
        end_time = datetime.now()
        
        config = AnalysisConfig(
            start_time=start_time,
            end_time=end_time,
            parallel_threads=5,
            histogram_mode=["pattern1", "pattern2"],
            node_filter=["node1", "node2"],
            log_type_filter=["postgres", "tserver"]
        )
        
        assert config.start_time == start_time
        assert config.end_time == end_time
        assert config.parallel_threads == 5
        assert config.histogram_mode == ["pattern1", "pattern2"]
        assert config.node_filter == ["node1", "node2"]
        assert config.log_type_filter == ["postgres", "tserver"]
    
    def test_analysis_config_defaults(self):
        """Test AnalysisConfig with default values."""
        start_time = datetime.now() - timedelta(days=1)
        end_time = datetime.now()
        
        config = AnalysisConfig(
            start_time=start_time,
            end_time=end_time
        )
        
        assert config.parallel_threads == 5
        assert config.histogram_mode is None
        assert config.node_filter is None
        assert config.log_type_filter is None 