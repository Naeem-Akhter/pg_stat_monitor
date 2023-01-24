#!/usr/bin/perl

use strict;
use warnings;
use File::Basename;
use File::Compare;
use File::Copy;
use String::Util qw(trim);
use Test::More;
use lib 't';
use pgsm;

# Get filename and create out file name and dirs where requried
PGSM::setup_files_dir(basename($0));

# Create new PostgreSQL node and do initdb
my $node = PGSM->pgsm_init_pg();
my $pgdata = $node->data_dir;

my $run_pg_sleep_function_sql = "CREATE OR REPLACE FUNCTION run_pg_sleep(INTEGER) RETURNS VOID AS \$\$
DECLARE
   iterator real := 0.000001;  -- we can init at declaration time
   loops ALIAS FOR \$1;
BEGIN
   WHILE iterator < loops
   LOOP
      RAISE INFO 'Current timestamp: %', timeofday()::TIMESTAMP;
      RAISE INFO 'Sleep % seconds', iterator;
	  PERFORM pg_sleep(iterator);
      iterator := iterator + iterator;
   END LOOP;
END;
\$\$ LANGUAGE 'plpgsql' STRICT;";

my $generate_histogram_function_sql = "CREATE OR REPLACE FUNCTION generate_histogram()
    RETURNS TABLE (
    range TEXT, freq INT, bar TEXT
  )  AS \$\$
Declare
    bucket_id integer;
    query_id bigint;
BEGIN
    select bucket into bucket_id from pg_stat_monitor order by calls desc limit 1;
    select queryid into query_id from pg_stat_monitor order by calls desc limit 1;
    --RAISE INFO 'bucket_id %', bucket_id;
    --RAISE INFO 'query_id %', query_id;
    return query
    SELECT * FROM histogram(bucket_id, query_id) AS a(range TEXT, freq INT, bar TEXT);
END;
\$\$ LANGUAGE plpgsql;";

# Update postgresql.conf to include/load pg_stat_monitor library   
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'pg_stat_monitor'");

# Set change postgresql.conf for this test case. 
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_bucket_time = 1800");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_min = 1");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_max = 10000");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_buckets = 3");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_normalized_query = yes");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_track = 'all'");

# Start server
my $rt_value = $node->start;
ok($rt_value == 1, "Start Server");

# Scenario 1. Run pg_sleep 
# Create functions required for testing
PGSM::append_to_file("==================Scenario 1==================");
my ($cmdret, $stdout, $stderr) = $node->psql('postgres', "$run_pg_sleep_function_sql", extra_params => ['-a']);
ok($cmdret == 0, "Create run_pg_sleep(INTEGER) function");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', "$generate_histogram_function_sql", extra_params => ['-a']);
ok($cmdret == 0, "Create generate_histogram() function");
PGSM::append_to_file($stdout);

# Create extension and change out file permissions
($cmdret, $stdout, $stderr) = $node->psql('postgres', 'CREATE EXTENSION pg_stat_monitor;', extra_params => ['-a']);
ok($cmdret == 0, "Create PGSM Extension");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', "SELECT name, setting, unit, context, vartype, source, min_val, max_val, enumvals, boot_val, reset_val, pending_restart FROM pg_settings WHERE name LIKE '%pg_stat_monitor%';", extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print PGSM EXTENSION Settings");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT pg_stat_monitor_reset();', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Reset PGSM Extension");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT run_pg_sleep(10);', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Run run_pg_sleep(10) to generate data for histogram testing");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT query, calls, resp_calls FROM pg_stat_monitor ORDER BY calls desc;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print what is in pg_stat_monitor view");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT bucket, queryid, query, calls, resp_calls FROM pg_stat_monitor ORDER BY calls desc;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print what is in pg_stat_monitor view");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT calls FROM pg_stat_monitor ORDER BY calls desc LIMIT 1;', extra_params => ['-Pformat=unaligned','-Ptuples_only=on']);
ok($cmdret == 0, "Get calls into a variable");
ok($stdout == 24, "Calls are 24");
my $calls_count = trim($stdout);
PGSM::append_to_file("Got Calls from query : " . $stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT resp_calls FROM pg_stat_monitor ORDER BY calls desc LIMIT 1;', extra_params => ['-Pformat=aligned','-Ptuples_only=on']);
ok($cmdret == 0, "Get resp_calls into a variable");
ok(trim($stdout) eq "{1,14,4,5,0}", "resp_calls is {1,14,4,5,0}");
my $resp_calls = trim($stdout);
PGSM::append_to_file("Got resp_calls from query : " . $stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT * from generate_histogram();', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Generate Histogram for pg_sleep");
PGSM::append_to_file($stdout);
like($stdout, qr/5 rows/, '5 rows are present in histogram output');

#Scenario 2
PGSM::append_to_file("\n\n==================Scenario 2==================");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_min = 20");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_max = 40");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_buckets = 20");
$node->restart();

($cmdret, $stdout, $stderr) = $node->psql('postgres', "SELECT name, setting, unit, context, vartype, source, min_val, max_val, enumvals, boot_val, reset_val, pending_restart FROM pg_settings WHERE name LIKE '%pg_stat_monitor%';", extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print PGSM EXTENSION Settings");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT pg_stat_monitor_reset();', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Reset PGSM Extension");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT 1 AS a;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Select 1");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT 1 AS a;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Select 1");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT query, calls, resp_calls FROM pg_stat_monitor ORDER BY calls desc;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print what is in pg_stat_monitor view");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT bucket, queryid, query, calls, resp_calls FROM pg_stat_monitor ORDER BY calls desc;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print what is in pg_stat_monitor view");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT calls FROM pg_stat_monitor ORDER BY calls desc LIMIT 1;', extra_params => ['-Pformat=unaligned','-Ptuples_only=on']);
ok($cmdret == 0, "Get calls into a variable");
ok($stdout == 2, "Calls are 2");
$calls_count = trim($stdout);
PGSM::append_to_file("Got Calls from query : " . $stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT resp_calls FROM pg_stat_monitor ORDER BY calls desc LIMIT 1;', extra_params => ['-Pformat=aligned','-Ptuples_only=on']);
ok($cmdret == 0, "Get resp_calls into a variable");
ok(trim($stdout) eq "{2,0,0,0,0,0,0,0,0,0}", "resp_calls is {2,0,0,0,0,0,0,0,0,0}");
$resp_calls = trim($stdout);
PGSM::append_to_file("Got resp_calls from query : " . $stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT * from generate_histogram();', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Generate Histogram for Select 1");
PGSM::append_to_file($stdout);
like($stdout, qr/10 rows/, '5 rows are present in histogram output');

#Scenario 3
PGSM::append_to_file("\n\n==================Scenario 3==================");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_min = 1000");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_max = 1010");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_buckets = 6");
$node->restart();

($cmdret, $stdout, $stderr) = $node->psql('postgres', "SELECT name, setting, unit, context, vartype, source, min_val, max_val, enumvals, boot_val, reset_val, pending_restart FROM pg_settings WHERE name LIKE '%pg_stat_monitor%';", extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print PGSM EXTENSION Settings");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT pg_stat_monitor_reset();', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Reset PGSM Extension");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT 1 AS a;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Select 1");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT 1 AS a;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Select 1");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT query, calls, resp_calls FROM pg_stat_monitor ORDER BY calls desc;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print what is in pg_stat_monitor view");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT bucket, queryid, query, calls, resp_calls FROM pg_stat_monitor ORDER BY calls desc;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print what is in pg_stat_monitor view");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT calls FROM pg_stat_monitor ORDER BY calls desc LIMIT 1;', extra_params => ['-Pformat=unaligned','-Ptuples_only=on']);
ok($cmdret == 0, "Get calls into a variable");
ok($stdout == 2, "Calls are 2");
$calls_count = trim($stdout);
PGSM::append_to_file("Got Calls from query : " . $stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT resp_calls FROM pg_stat_monitor ORDER BY calls desc LIMIT 1;', extra_params => ['-Pformat=aligned','-Ptuples_only=on']);
ok($cmdret == 0, "Get resp_calls into a variable");
ok(trim($stdout) eq "{2,0,0,0,0,0,0,0}", "resp_calls is {2,0,0,0,0,0,0,0}");
$resp_calls = trim($stdout);
PGSM::append_to_file("Got resp_calls from query : " . $stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT * from generate_histogram();', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Generate Histogram for Select 1");
PGSM::append_to_file($stdout);
like($stdout, qr/8 rows/, '5 rows are present in histogram output');

#Scenario 4
PGSM::append_to_file("\n\n==================Scenario 4==================");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_min = 0");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_max = 2147483647");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_buckets = 20");
$node->restart();

($cmdret, $stdout, $stderr) = $node->psql('postgres', "SELECT name, setting, unit, context, vartype, source, min_val, max_val, enumvals, boot_val, reset_val, pending_restart FROM pg_settings WHERE name LIKE '%pg_stat_monitor%';", extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print PGSM EXTENSION Settings");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT pg_stat_monitor_reset();', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Reset PGSM Extension");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT 1 AS a;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Select 1");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT 1 AS a;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Select 1");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT query, calls, resp_calls FROM pg_stat_monitor ORDER BY calls desc;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print what is in pg_stat_monitor view");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT bucket, queryid, query, calls, resp_calls FROM pg_stat_monitor ORDER BY calls desc;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print what is in pg_stat_monitor view");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT calls FROM pg_stat_monitor ORDER BY calls desc LIMIT 1;', extra_params => ['-Pformat=unaligned','-Ptuples_only=on']);
ok($cmdret == 0, "Get calls into a variable");
ok($stdout == 2, "Calls are 2");
$calls_count = trim($stdout);
PGSM::append_to_file("Got Calls from query : " . $stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT resp_calls FROM pg_stat_monitor ORDER BY calls desc LIMIT 1;', extra_params => ['-Pformat=aligned','-Ptuples_only=on']);
ok($cmdret == 0, "Get resp_calls into a variable");
ok(trim($stdout) eq "{2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}", "resp_calls is {2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}");
$resp_calls = trim($stdout);
PGSM::append_to_file("Got resp_calls from query : " . $stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT * from generate_histogram();', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Generate Histogram for Select 1");
PGSM::append_to_file($stdout);
like($stdout, qr/21 rows/, '5 rows are present in histogram output');

#Scenario 5
PGSM::append_to_file("\n\n==================Scenario 5==================");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_min = 1");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_max = 10");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_buckets = 3");
$node->restart();

($cmdret, $stdout, $stderr) = $node->psql('postgres', "SELECT name, setting, unit, context, vartype, source, min_val, max_val, enumvals, boot_val, reset_val, pending_restart FROM pg_settings WHERE name LIKE '%pg_stat_monitor%';", extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print PGSM EXTENSION Settings");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT pg_stat_monitor_reset();', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Reset PGSM Extension");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT 1 AS a;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Select 1");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT 1 AS a;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Select 1");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT query, calls, resp_calls FROM pg_stat_monitor ORDER BY calls desc;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print what is in pg_stat_monitor view");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT bucket, queryid, query, calls, resp_calls FROM pg_stat_monitor ORDER BY calls desc;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print what is in pg_stat_monitor view");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT calls FROM pg_stat_monitor ORDER BY calls desc LIMIT 1;', extra_params => ['-Pformat=unaligned','-Ptuples_only=on']);
ok($cmdret == 0, "Get calls into a variable");
ok($stdout == 2, "Calls are 2");
$calls_count = trim($stdout);
PGSM::append_to_file("Got Calls from query : " . $stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT resp_calls FROM pg_stat_monitor ORDER BY calls desc LIMIT 1;', extra_params => ['-Pformat=aligned','-Ptuples_only=on']);
ok($cmdret == 0, "Get resp_calls into a variable");
ok(trim($stdout) eq "{2,0,0,0,0}", "resp_calls is {2,0,0,0,0}");
$resp_calls = trim($stdout);
PGSM::append_to_file("Got resp_calls from query : " . $stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT * from generate_histogram();', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Generate Histogram for Select 1");
PGSM::append_to_file($stdout);
like($stdout, qr/5 rows/, '5 rows are present in histogram output');

#Scenario 6
PGSM::append_to_file("\n\n==================Scenario 6==================");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_min = 0");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_max = 2");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_buckets = 2");
$node->restart();

($cmdret, $stdout, $stderr) = $node->psql('postgres', "SELECT name, setting, unit, context, vartype, source, min_val, max_val, enumvals, boot_val, reset_val, pending_restart FROM pg_settings WHERE name LIKE '%pg_stat_monitor%';", extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print PGSM EXTENSION Settings");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT pg_stat_monitor_reset();', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Reset PGSM Extension");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT 1 AS a;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Select 1");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT 1 AS a;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Select 1");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT query, calls, resp_calls FROM pg_stat_monitor ORDER BY calls desc;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print what is in pg_stat_monitor view");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT bucket, queryid, query, calls, resp_calls FROM pg_stat_monitor ORDER BY calls desc;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print what is in pg_stat_monitor view");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT calls FROM pg_stat_monitor ORDER BY calls desc LIMIT 1;', extra_params => ['-Pformat=unaligned','-Ptuples_only=on']);
ok($cmdret == 0, "Get calls into a variable");
ok($stdout == 2, "Calls are 2");
$calls_count = trim($stdout);
PGSM::append_to_file("Got Calls from query : " . $stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT resp_calls FROM pg_stat_monitor ORDER BY calls desc LIMIT 1;', extra_params => ['-Pformat=aligned','-Ptuples_only=on']);
ok($cmdret == 0, "Get resp_calls into a variable");
ok(trim($stdout) eq "{2,0,0}", "resp_calls is {2,0,0}");
$resp_calls = trim($stdout);
PGSM::append_to_file("Got resp_calls from query : " . $stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT * from generate_histogram();', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Generate Histogram for Select 1");
PGSM::append_to_file($stdout);
like($stdout, qr/3 rows/, '5 rows are present in histogram output');

# Drop extension
$stdout = $node->safe_psql('postgres', 'Drop extension pg_stat_monitor;',  extra_params => ['-a']);
ok($cmdret == 0, "Drop PGSM  Extension");
PGSM::append_to_file($stdout);

# Stop the server
$node->stop;

# Done testing for this testcase file.
done_testing();
