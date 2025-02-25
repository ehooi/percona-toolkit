#!/usr/bin/env perl

BEGIN {
   die "The PERCONA_TOOLKIT_BRANCH environment variable is not set.\n"
      unless $ENV{PERCONA_TOOLKIT_BRANCH} && -d $ENV{PERCONA_TOOLKIT_BRANCH};
   unshift @INC, "$ENV{PERCONA_TOOLKIT_BRANCH}/lib";
};

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use Test::More;

use PerconaTest;
use Sandbox;
require "$trunk/bin/pt-config-diff";

require VersionParser;
my $dp = new DSNParser(opts=>$dsn_opts);
my $sb = new Sandbox(basedir => '/tmp', DSNParser => $dp);
my $source_dbh = $sb->get_dbh_for('source');
my $replica_dbh  = $sb->get_dbh_for('replica1');

if ( !$source_dbh ) {
   plan skip_all => 'Cannot connect to sandbox source';
}
elsif ( !$replica_dbh ) {
   plan skip_all => 'Cannot connect to sandbox replica';
}
else {
   plan tests => 3;
}

my $cnf = '/tmp/12345/my.sandbox.cnf';
my $output;
my $retval;

# ############################################################################
# Report stuff.
# ############################################################################

$output = output(
   sub { $retval = pt_config_diff::main(
      'h=127.1,P=12345,u=msandbox,p=msandbox', 'P=12346',
      '--no-report')
   },
   stderr => 1,
);

is(
   $retval,
   1,
   "Diff but no report"
);

is(
   $output,
   "",
   "Diff but not output because --no-report"
);

# #############################################################################
# Done.
# #############################################################################
ok($sb->ok(), "Sandbox servers") or BAIL_OUT(__FILE__ . " broke the sandbox");
exit;
