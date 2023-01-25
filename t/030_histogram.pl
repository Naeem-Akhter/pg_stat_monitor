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

sub generate_histogram_with_configurations
{
   PGSM::append_to_file("\n\n===============Start===============");

   my ($h_min, $h_max, $h_buckets, $expected_calls_count, $expected_resp_calls, $expected_histogram_rows, $use_pg_sleep) = @_;
   PGSM::append_to_file("pgsm_histogram_min : " . $h_min);
   PGSM::append_to_file("pgsm_histogram_max : " . $h_max);
   PGSM::append_to_file("pgsm_histogram_buckets : " . $h_buckets);
   PGSM::append_to_file("expected total calls count : " . $expected_calls_count);
   PGSM::append_to_file("expected resp_calls value : " . $expected_resp_calls);
   PGSM::append_to_file("expected ranges (rows) count in histogram: " . $expected_histogram_rows);
   PGSM::append_to_file("using pg_sleep to generate dataset : " . $use_pg_sleep);

   $node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_min = $h_min");
   $node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_max = $h_max");
   $node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_histogram_buckets = $h_buckets");
   $node->restart();

   my ($cmdret, $stdout, $stderr) = $node->psql('postgres', "SELECT name, setting, unit, context, vartype, source, min_val, max_val, enumvals, boot_val, reset_val, pending_restart FROM pg_settings WHERE name LIKE '%pg_stat_monitor%';", extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
   ok($cmdret == 0, "Print PGSM EXTENSION Settings");
   PGSM::append_to_file($stdout);

   ($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT pg_stat_monitor_reset();', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
   ok($cmdret == 0, "Reset PGSM Extension");
   PGSM::append_to_file($stdout);

   if($use_pg_sleep == 1)
   {
      ($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT run_pg_sleep(10);', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
      ok($cmdret == 0, "Run run_pg_sleep(10) to generate data for histogram testing");
      PGSM::append_to_file($stdout);
   }
   else
   {
      for (my $count = 1 ; $count <= $expected_calls_count ; $count++)
      {
         ($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT 1 AS a;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
         ok($cmdret == 0, "SELECT 1 AS a");
         PGSM::append_to_file($stdout);
      }
   }

   ($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT bucket, queryid, query, calls, resp_calls FROM pg_stat_monitor ORDER BY calls desc;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
   ok($cmdret == 0, "Print what is in pg_stat_monitor view");
   PGSM::append_to_file($stdout);

   ($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT calls FROM pg_stat_monitor ORDER BY calls desc LIMIT 1;', extra_params => ['-Pformat=unaligned','-Ptuples_only=on']);
   ok($cmdret == 0, "Get calls into a variable");
   ok($stdout == $expected_calls_count, "Calls are $expected_calls_count");
   my $calls_count = trim($stdout);
   PGSM::append_to_file("Got Calls from query : " . $stdout);

   ($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT resp_calls FROM pg_stat_monitor ORDER BY calls desc LIMIT 1;', extra_params => ['-Pformat=aligned','-Ptuples_only=on']);
   ok($cmdret == 0, "Get resp_calls into a variable");
   ok(trim($stdout) eq "$expected_resp_calls", "resp_calls is $expected_resp_calls");
   my $resp_calls = trim($stdout);
   PGSM::append_to_file("Got resp_calls from query : " . $stdout);

   ($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT * from generate_histogram();', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
   ok($cmdret == 0, "Generate Histogram for Select 1");
   PGSM::append_to_file($stdout);
   like($stdout, qr/$expected_histogram_rows rows/, "$expected_histogram_rows rows are present in histogram output");

   PGSM::append_to_file("===============End===============");
}

# Update postgresql.conf to include/load pg_stat_monitor library   
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'pg_stat_monitor'");

# Set change postgresql.conf for this test case. 
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_bucket_time = 1800");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_normalized_query = yes");
$node->append_conf('postgresql.conf', "pg_stat_monitor.pgsm_track = 'all'");

# Start server
my $rt_value = $node->start;
ok($rt_value == 1, "Start Server");

# Create functions required for testing
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

# Following parameters are required for function 'generate_histogram_with_configurations' to generate and test a histogram
# with given configuration. 
# Parameter 1 ==> pgsm_histogram_min
# Parameter 2 ==> pgsm_histogram_max
# Parameter 3 ==> pgsm_histogram_buckets
# Parameter 4 ==> generated and expected total calls count against 'SELECT 1 AS a' 
# Parameter 5 ==> expected resp_calls value output
# Parameter 6 ==> expected ranges (rows) count in histogram output
# Parameter 7 ==> using pg_sleep to generate dataset, '1' will call sql function 'run_pg_sleep' (declared above).
#                 '0' will call the 'SELECT 1 AS a' expected_total_calls (Parameter 4) times. 

# Scenario 1. Run pg_sleep 
generate_histogram_with_configurations(1, 10000, 3, 24, "{1,14,4,5,0}", 5, 1);

#Scenario 2
generate_histogram_with_configurations(20, 40, 20, 2, "{2,0,0,0,0,0,0,0,0,0}", 10, 0);

#Scenario 3
generate_histogram_with_configurations(1000, 1010, 6, 2, "{2,0,0,0,0,0,0,0}", 8, 0);

#Scenario 4
generate_histogram_with_configurations(0, 2147483647, 20, 2, "{2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}", 21, 0);

#Scenario 5
generate_histogram_with_configurations(1, 10, 3, 2, "{2,0,0,0,0}", 5, 0);

#Scenario 6
generate_histogram_with_configurations(0, 2, 2, 2, "{2,0,0}", 3, 0);

# Drop extension
$stdout = $node->safe_psql('postgres', 'Drop extension pg_stat_monitor;',  extra_params => ['-a']);
ok($cmdret == 0, "Drop PGSM  Extension");
PGSM::append_to_file($stdout);

# Stop the server
$node->stop;

# Done testing for this testcase file.
done_testing();
