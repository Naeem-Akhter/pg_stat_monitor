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
    query_id text;
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

# Run 'SELECT * from pg_stat_monitor_settings;' two times and dump output to out file 
($cmdret, $stdout, $stderr) = $node->psql('postgres', 'Select name, value, default_value, minimum, maximum, options, restart from pg_stat_monitor_settings;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print PGSM Extension Settings");
PGSM::append_to_file($stdout);

# Run required commands/queries and dump output to out file.
($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT pg_stat_monitor_reset();', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Reset PGSM Extension");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT run_pg_sleep(10);', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Run run_pg_sleep(10) to generate data for histogram testing");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT bucket, queryid, query, calls, resp_calls FROM pg_stat_monitor ORDER BY calls;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print what is in pg_stat_monitor view");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT substr(query, 0,50) as query, calls, resp_calls FROM pg_stat_monitor ORDER BY calls;', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Print what is in pg_stat_monitor view");
PGSM::append_to_file($stdout);

($cmdret, $stdout, $stderr) = $node->psql('postgres', 'SELECT * from generate_histogram();', extra_params => ['-a', '-Pformat=aligned','-Ptuples_only=off']);
ok($cmdret == 0, "Generate Histogram for pg_sleep");
PGSM::append_to_file($stdout);
like($stdout, qr/3 rows/, '3 rows are present in histogram output');

# Drop extension
$stdout = $node->safe_psql('postgres', 'Drop extension pg_stat_monitor;',  extra_params => ['-a']);
ok($cmdret == 0, "Drop PGSM  Extension");
PGSM::append_to_file($stdout);

# Stop the server
$node->stop;

# Done testing for this testcase file.
done_testing();
