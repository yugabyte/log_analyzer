import yaml
import os

##############################################################################
# Read log_conf.yml and parse into patterns/solutions for universe & pg
##############################################################################
config_path = os.path.join(os.path.dirname(__file__), "..", "log_conf.yml")
with open(config_path, "r") as f:
	config = yaml.safe_load(f)

universe_config = config["universe"]["log_messages"]
pg_config = config["pg"]["log_messages"]

universe_regex_patterns = {}
universe_solutions = {}
for msg_dict in universe_config:
    name = msg_dict["name"]
    pattern = msg_dict["pattern"]
    solution = msg_dict["solution"]
    universe_regex_patterns[name] = pattern
    universe_solutions[name] = solution

pg_regex_patterns = {}
pg_solutions = {}
for msg_dict in pg_config:
    name = msg_dict["name"]
    pattern = msg_dict["pattern"]
    solution = msg_dict["solution"]
    pg_regex_patterns[name] = pattern
    pg_solutions[name] = solution

# Merge them for easy usage in log_analyzer
solutions = {**universe_solutions, **pg_solutions}