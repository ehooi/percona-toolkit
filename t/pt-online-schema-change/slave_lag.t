#!/usr/bin/env perl

BEGIN {
   die "The PERCONA_TOOLKIT_BRANCH environment variable is not set.\n"
      unless $ENV{PERCONA_TOOLKIT_BRANCH} && -d $ENV{PERCONA_TOOLKIT_BRANCH};
   unshift @INC, "$ENV{PERCONA_TOOLKIT_BRANCH}/lib";
};

use strict;
use warnings FATAL => 'all';
use threads;

use English qw(-no_match_vars);
use Test::More;

use Data::Dumper;
use PerconaTest;
use Sandbox;
use SqlModes;
use File::Temp qw/ tempdir tempfile /;

if ($ENV{PERCONA_SLOW_BOX}) {
    plan skip_all => 'This test needs a fast machine';
} 

our $delay = 30;

my $tmp_file = File::Temp->new();
my $tmp_file_name = $tmp_file->filename;
unlink $tmp_file_name;

require "$trunk/bin/pt-online-schema-change";

my $dp = new DSNParser(opts=>$dsn_opts);
my $sb = new Sandbox(basedir => '/tmp', DSNParser => $dp);
if ($sb->is_cluster_mode) {
    plan skip_all => 'Not for PXC';
} else {
    plan tests => 6;
}                                  
my $source_dbh = $sb->get_dbh_for('source');
my $replica_dbh = $sb->get_dbh_for('replica1');
my $source_dsn = 'h=127.0.0.1,P=12345,u=msandbox,p=msandbox';
my $replica_dsn = 'h=127.0.0.1,P=12346,u=msandbox,p=msandbox';

sub reset_query_cache {
    my @dbhs = @_;
    return if ($sandbox_version >= '8.0');
    foreach my $dbh (@dbhs) {
        $dbh->do('RESET QUERY CACHE');
    }
}

# 1) Set the replica delay to 0 just in case we are re-running the tests without restarting the sandbox.
# 2) Load sample data
# 3) Set the replica delay to 30 seconds to be able to see the 'waiting' message.
diag("Setting replica delay to 0 seconds");
$replica_dbh->do("STOP ${replica_name}");
$source_dbh->do("RESET ${source_reset}");
$replica_dbh->do("RESET ${replica_name}");
$replica_dbh->do("START ${replica_name}");

diag('Loading test data');
$sb->load_file('source', "t/pt-online-schema-change/samples/replica_lag.sql");

# Should be greater than chunk-size and big enough, so pt-osc will wait for delay
my $num_rows = 5000;
diag("Loading $num_rows into the table. This might take some time.");
diag(`util/mysql_random_data_load --host=127.0.0.1 --port=12345 --user=msandbox --password=msandbox test pt178 $num_rows`);

$sb->wait_for_replicas();
diag("Setting replica delay to $delay seconds");

$replica_dbh->do("STOP ${replica_name}");
$replica_dbh->do("CHANGE ${source_change} TO ${source_name}_DELAY=$delay");
$replica_dbh->do("START ${replica_name}");

# Run a full table scan query to ensure the replica is behind the source
# There is no query cache in MySQL 8.0+
reset_query_cache($source_dbh, $source_dbh);
# Update one row so replica is delayed
$source_dbh->do('UPDATE `test`.`pt178` SET f2 = f2 + 1 LIMIT 1');
$source_dbh->do('UPDATE `test`.`pt178` SET f2 = f2 + 1 WHERE f1 = ""');

# This is the base test, just to ensure that without using --check-replica-lag nor --skip-check-replica-lag
# pt-online-schema-change will wait on the replica at port 12346

my $max_lag = $delay / 2;
# We need to sleep, otherwise pt-osc can finish before replica is delayed
sleep($max_lag);

my $args = "$source_dsn,D=test,t=pt178 --execute --chunk-size 10 --max-lag $max_lag --alter 'ENGINE=InnoDB' --pid $tmp_file_name --progress time,5";
diag("Starting base test. This is going to take some time due to the delay in the replica");
diag("pid: $tmp_file_name");
my $output = `$trunk/bin/pt-online-schema-change $args 2>&1`;
like(
      $output,
      qr/Replica lag is \d+ seconds on .*  Waiting/s,
      "Base test waits on the correct replica",
);

# Repeat the test now using --check-replica-lag
$args = "$source_dsn,D=test,t=pt178 --execute --chunk-size 1 --max-lag $max_lag --alter 'ENGINE=InnoDB' "
      . "--check-replica-lag h=127.0.0.1,P=12346,u=msandbox,p=msandbox,D=test,t=sbtest --pid $tmp_file_name --progress time,5";

# Run a full table scan query to ensure the replica is behind the source
reset_query_cache($source_dbh, $source_dbh);
# Update one row so replica is delayed
$source_dbh->do('UPDATE `test`.`pt178` SET f2 = f2 + 1 LIMIT 1');
$source_dbh->do('UPDATE `test`.`pt178` SET f2 = f2 + 1 WHERE f1 = ""');

# We need to sleep, otherwise pt-osc can finish before replica is delayed
sleep($max_lag);
diag("Starting --check-replica-lag test. This is going to take some time due to the delay in the replica");
$output = `$trunk/bin/pt-online-schema-change $args 2>&1`;

like(
      $output,
      qr/Replica lag is \d+ seconds on .*  Waiting/s,
      "--check-replica-lag waits on the correct replica",
);

# Repeat the test new adding and removing a replica during the process
$args = "$source_dsn,D=test,t=pt178 --execute --chunk-size 1 --max-lag $max_lag --alter 'ENGINE=InnoDB' "
      . "--recursion-method=dsn=D=test,t=dynamic_replicas --recurse 0 --pid $tmp_file_name --progress time,5";

$source_dbh->do('CREATE TABLE `test`.`dynamic_replicas` (id INTEGER PRIMARY KEY, dsn VARCHAR(255) )');
$source_dbh->do("INSERT INTO `test`.`dynamic_replicas` (id, dsn) VALUES (1, '$replica_dsn')");

# Run a full table scan query to ensure the replica is behind the source
reset_query_cache($source_dbh, $source_dbh);
# Update one row so replica is delayed
$source_dbh->do('UPDATE `test`.`pt178` SET f2 = f2 + 1 LIMIT 1');
$source_dbh->do('UPDATE `test`.`pt178` SET f2 = f2 + 1 WHERE f1 = ""');

# We need to sleep, otherwise pt-osc can finish before replica is delayed
sleep($max_lag);
diag("Starting --recursion-method with changes during the process");
my ($fh, $filename) = tempfile();
my $pid = fork();

if (!$pid) {
    open(STDERR, '>', $filename);
    open(STDOUT, '>', $filename);
    exec("$trunk/bin/pt-online-schema-change $args");
}

sleep($max_lag + 10);
$source_dbh->do("DELETE FROM `test`.`dynamic_replicas` WHERE id = 1;");
waitpid($pid, 0);
$output = do {
      local $/ = undef;
      <$fh>;
};

unlink $filename;

like(
      $output,
      qr/Replica set to watch has changed/s,
      "--recursion-method=dsn updates the replica list",
) or diag($output);

like(
      $output,
      qr/Replica lag is \d+ seconds on .*  Waiting/s,
      "--recursion-method waits on a replica",
) or diag($output);

# Repeat the test now using --skip-check-replica-lag
# Run a full table scan query to ensure the replica is behind the source
reset_query_cache($source_dbh, $source_dbh);
# Update one row so replica is delayed
$source_dbh->do('UPDATE `test`.`pt178` SET f2 = f2 + 1 LIMIT 1');
$source_dbh->do('UPDATE `test`.`pt178` SET f2 = f2 + 1 WHERE f1 = ""');

# We need to sleep, otherwise pt-osc can finish before replica is delayed
sleep($max_lag);
$args = "$source_dsn,D=test,t=pt178 --execute --chunk-size 1 --max-lag $max_lag --alter 'ENGINE=InnoDB' "
      . "--skip-check-replica-lag h=127.0.0.1,P=12346,u=msandbox,p=msandbox,D=test,t=sbtest --pid $tmp_file_name --progress time,5";

diag("Starting --skip-check-replica-lag test. This is going to take some time due to the delay in the replica");
$output = `$trunk/bin/pt-online-schema-change $args 2>&1`;

unlike(
      $output,
      qr/Replica lag is \d+ seconds on .*:12346.  Waiting/s,
      "--skip-check-replica-lag is really skipping the replica",
);

diag("Setting replica delay to 0 seconds");
$replica_dbh->do("STOP ${replica_name}");
$source_dbh->do("RESET ${source_reset}");
$replica_dbh->do("RESET ${replica_name}");
$replica_dbh->do("START ${replica_name}");

$source_dbh->do("DROP DATABASE IF EXISTS test");
$sb->wait_for_replicas();

# #############################################################################
# Done.
# #############################################################################
$sb->wipe_clean($source_dbh);
ok($sb->ok(), "Sandbox servers") or BAIL_OUT(__FILE__ . " broke the sandbox");
done_testing;
