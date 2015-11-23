package GRNOC::TSDS::Views::Daemon;

use strict;
use warnings;

use Moo;
use Types::Standard qw( Str Bool );

use GRNOC::Config;
use GRNOC::Log;

use Parallel::ForkManager;
use Proc::Daemon;

use Data::Dumper;
use MongoDB;

use Net::AMQP::RabbitMQ;
use JSON::XS;

extends 'GRNOC::TSDS::Views';

### public methods ###

sub start {

    my ( $self ) = @_;

    log_info( 'Starting TSDS Views daemon.' );

    if (! $self->config ){
        die "Unable to load config file";
    }

    log_debug( 'Setting up signal handlers.' );

    # setup signal handlers
    $SIG{'TERM'} = sub {
        log_info( 'Received SIG TERM.' );
        $self->stop();
    };

    $SIG{'HUP'} = sub {
        log_info( 'Received SIG HUP.' );
    };

    # need to daemonize
    if ( $self->daemonize ) {

        log_debug( 'Daemonizing.' );

        my $daemon = Proc::Daemon->new( pid_file => $self->config->get( '/config/pid-file' ) );

        my $pid = $daemon->Init();

        # in child/daemon process
        if ( $pid ){
            log_debug(" Forked child $pid, exiting process ");
            return;
        }

        log_debug( 'Created daemon process.' );
    }

    # dont need to daemonize
    else {

        log_debug( 'Running in foreground.' );
    }

    $self->_mongo_connect() or return;

    $self->_rabbit_connect() or return;

    log_debug("Entering main work loop");

    $self->_work_loop();

    return 1;
}

sub _work_loop {
    my ( $self ) = @_;

    while (1){

        my $next_wake_time;
    
        my $now = time;

        # Find the databases that need workings
        my $dbs = $self->_get_derived_databases();

        if ((keys %$dbs) == 0){
            log_info("No derived databases to work on, sleeping for 60s...");
            sleep(60);
            next;
        }

        # For each of those databases, determine whether
        # it's time to do work yet or not
        foreach my $db_name (keys %$dbs){
            my $source = $dbs->{$db_name};

            my $interval = $source->{'interval'};
            my $last_run = $source->{'last_run'} || 0;

            my $next_run = $last_run + $interval;

            # If there's work to do, let's craft a work
            # order out to some worker and send it
            if ($next_run <= $now){
                my $result = $self->_generate_work($source);
                if (! defined $result){
                    log_warn("Error generating work for $db_name, skipping");
                    next;
                }

                # Update the derived database to show that we have handled this time
                # period and sent out a work order
                $self->mongo->get_database($db_name)->get_collection("metadata")->update({}, 
                                                                                         {'$set' => {"source.last_run" => $next_run}});                

                $next_run += $interval;
            }

            log_debug("Next run is $next_run for $db_name");

            # Figure out when the next time we need to look at this is.
            # If it's closer than anything else, update our next wake 
            # up time to that
            if (! defined $next_wake_time || $next_run < $next_wake_time){
                $next_wake_time = $next_run;
            }
        }
       
        log_debug("Next wake time is $next_wake_time");

        # Sleep until the next time we've determined we need to do something
        my $delta = $next_wake_time - time;
        if ($delta > 0){
            log_info("Sleeping $delta seconds until next work");
            sleep($delta);
        }
        else {
            log_debug("Not sleeping since delta <= 0");
        }
    }
}


# Formulate and send a message out to a worker
sub _generate_work {
    my ( $self, $source_info ) = @_;

    # First we need to validate all the info looks good to formulate the query

    my $last_run = $source_info->{'last_run'};
    my $start    = $last_run;
    my $end      = $last_run + $source_info->{'interval'};

    my $message = [
        {            
            to       => $source_info->{'to'},
            start    => $start,
            end      => $end, 
            interval => $source_info->{'interval'},
            from     => $source_info->{'from'},
            fields   => $source_info->{'fields'},
            where    => $source_info->{'where'},
            by       => $source_info->{'by'},
            having   => $source_info->{'having'},
            order    => $source_info->{'order'},
            limit    => $source_info->{'limit'},
            offset   => $source_info->{'offset'},
            meta     => $source_info->{'meta'}
        }
        ];

    log_debug("Sending message to rabbit for " . $source_info->{'to'} . " from $start to $end");

    $self->rabbit->publish(1, $self->rabbit_queue, encode_json($message), {'exchange' => ''});

    return 1;
}


sub _get_derived_databases {
    my ( $self ) = @_;

    my @db_names = $self->mongo->database_names;

    my %derived;

    foreach my $db_name (@db_names){
        my $metadata;
        eval {
            $metadata = $self->mongo->get_database($db_name)->get_collection("metadata")->find_one();
        };
        if ($@){
            if ($@ !~ /not authorized/){
                log_warn("Error querying mongo: $@");
            }
            next;
        }

        # Does this metadata doc have a "source" attribute? If so,
        # it's a derived view in TSDS from some other database
        # so let's add it
        if ($metadata && $metadata->{'source'}){
            my $source = $metadata->{'source'};
            $source->{'to'} = $db_name;
            $self->_validate_view($source) or next;
            $derived{$db_name} = $source;
        }
    }

    log_debug("Found derived databases: " . Dumper(\%derived));

    return \%derived;
}

1;
