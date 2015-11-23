package GRNOC::TSDS::Views::Worker;

use Moo;
use Types::Standard qw( Str Bool );

use Data::Dumper;

use GRNOC::Config;
use GRNOC::Log;

use GRNOC::TSDS::Views::Worker::Child;

use Parallel::ForkManager;
use Proc::Daemon;

extends 'GRNOC::TSDS::Views';

### extended private attributes ###

has children => ( is => 'rwp',
                  default => sub { [] } );

sub start {

    my ( $self ) = @_;

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

    $self->_make_babies();

    return 1;
}

sub _make_babies {
    my ( $self ) = @_;

    my $num_processes = $self->config->get( '/config/worker/num-processes' );

    log_info( "Creating $num_processes child worker processes." );

    my $forker = Parallel::ForkManager->new( $num_processes );

    # keep track of children pids
    $forker->run_on_start( 
        sub {            
            my ( $pid ) = @_;
            
            log_debug( "Child worker process $pid created." );
            
            push( @{$self->children}, $pid );
        } 
        );
    
    for ( 1 .. $num_processes ) {
        
        $forker->start() and next;
        
        # create worker in this process
        my $worker = GRNOC::TSDS::Views::Worker::Child->new( config_file => $self->config_file );
        
        # this should only return if we tell it to stop via TERM signal etc.
        $worker->start();
        
        # exit child process
        $forker->finish();
    }

    log_debug( 'Waiting for all child worker processes to exit.' );

    # wait for all children to return
    $forker->wait_all_children();

    $self->_set_children( [] );

    log_debug( 'All child workers have exited.' );
}

1;
