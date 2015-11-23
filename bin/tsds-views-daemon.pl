#!/usr/bin/perl

use strict;
use warnings;

use GRNOC::TSDS::Views::Daemon;
use JSON::XS;

use GRNOC::Log;
use Getopt::Long;
use Data::Dumper;

### constants ###

use constant DEFAULT_CONFIG_FILE => '/etc/grnoc/tsds-views/config.xml';
use constant DEFAULT_LOGGING_FILE => '/etc/grnoc/tsds-views/logging.conf';
use constant DEFAULT_LOCK_DIR => '/var/run/grnoc/tsds-views/';
use constant DEFAULT_NUM_PROCESSES => 4;

### command line options ###

my $help;
my $config = DEFAULT_CONFIG_FILE;
my $logging = DEFAULT_LOGGING_FILE;
my $lock_dir = DEFAULT_LOCK_DIR;
my $nofork;

# parse options from command line
GetOptions( "help|h|?"    => \$help,
            "config=s"    => \$config,
            "logging=s"   => \$logging,
            "lock-dir=s"  => \$lock_dir,
            "nofork"      => \$nofork
           ) or die;

# did they ask for help?
usage() if $help;

GRNOC::Log->new(config => $logging);

my $views = GRNOC::TSDS::Views::Daemon->new(config_file  => $config,
                                            lock_dir     => $lock_dir,
                                            daemonize    => !$nofork);

$views->start();                                    

sub usage {
    print "Usage: $0 [--config <path>] [--logging <path>] [--lock-dir <path>] [--help]\n";
    exit(1);
}
