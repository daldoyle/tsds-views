package GRNOC::TSDS::Views;

use strict;
use warnings;

use Moo;
use Types::Standard qw( Str Bool );

use GRNOC::Log;
use Data::Dumper;

use MongoDB;

### required attributes ###

has config_file => ( is => 'ro',
                     isa => Str,
                     required => 1 );


### optional attributes ###

has daemonize => ( is => 'ro',
                   isa => Bool,
                   default => 1 );

### private attributes ###

has config => ( is => 'rwp' );

has mongo => ( is => 'rwp' );

has rabbit => ( is => 'rwp' );

has rabbit_queue => ( is => 'rwp' );

sub BUILD {

    my ( $self ) = @_;

    # create and store config object
    my $config = GRNOC::Config->new( config_file => $self->config_file,
                                     force_array => 0 );


    $self->_set_config( $config );   

    # setup signal handlers
    $SIG{'TERM'} = sub {
        log_info( 'Received SIG TERM.' );
        $self->stop();
    };

    $SIG{'HUP'} = sub {
        log_info( 'Received SIG HUP.' );
    };


    return $self;
}

sub start {
    die "Must be implemented by subclass";
}

sub _mongo_connect {
    my ( $self ) = @_;

    my $mongo_host = $self->config->get( '/config/mongo/@host' );
    my $mongo_port = $self->config->get( '/config/mongo/@port' );
    my $user       = $self->config->get( '/config/mongo/@username' );
    my $pass       = $self->config->get( '/config/mongo/@password' );
    
    log_debug( "Connecting to MongoDB as $user:$pass on $mongo_host:$mongo_port." );

    my $mongo;
    eval {
        $mongo = MongoDB::MongoClient->new(
            host => "$mongo_host:$mongo_port",
            query_timeout => -1,
            username => $user,
            password => $pass
            );
    };
    if($@){
        log_warn("Could not connect to Mongo: $@");
        return;
    }

    log_debug("Connected");

    $self->_set_mongo( $mongo );
}

sub _rabbit_connect {
    my ( $self ) = @_;

    my $rabbit = Net::AMQP::RabbitMQ->new();   

    my $rabbit_host = $self->config->get( '/config/rabbit/@host' );
    my $rabbit_port = $self->config->get( '/config/rabbit/@port' );
    my $rabbit_queue = $self->config->get( '/config/rabbit/@queue' );

    log_debug("Connecting to RabbitMQ on $rabbit_host:$rabbit_port with queue $rabbit_queue");

    my $rabbit_args = {'port' => $rabbit_port};

    eval {
        $rabbit->connect( $rabbit_host, $rabbit_args );
        $rabbit->channel_open( 1 );
        $rabbit->queue_declare( 1, $rabbit_queue, {'auto_delete' => 0} );
    };
    if ($@){
        log_warn("Unable to connect to RabbitMQ: $@");
        return;
    }

    $self->_set_rabbit_queue($rabbit_queue);
    $self->_set_rabbit($rabbit);

    log_debug("Connected");

    return 1;
}

sub _validate_view {
    my ( $self, $view_info ) = @_;

    if (ref $view_info ne 'HASH'){
        log_warn("View info must be a hash");
        return;
    }

    log_debug("View info is: " . Dumper($view_info));

    my $db_name  = $view_info->{'to'};
    my $interval = $view_info->{'interval'};
    my $from     = $view_info->{'from'};
    my $fields   = $view_info->{'fields'};
    my $where    = $view_info->{'where'};
    my $by       = $view_info->{'by'};
    my $having   = $view_info->{'having'};
    my $order    = $view_info->{'order'};
    my $limit    = $view_info->{'limit'};
    my $offset   = $view_info->{'offset'};
    my $metadata = $view_info->{'meta'};

    if (! defined $db_name){
        log_warn("Source for $db_name is missing the \"database\" field");
        return;
    }

    # So much verification
    if (! defined $interval || $interval !~ /^\d+/ || $interval < 1){
        log_warn("Interval for $db_name is invalid, must be integer > 1");
        return;
    }

    # Make sure that the "from" database is defined and that it actually exists
    if (! $from){
        log_warn("Source for $db_name is missing the \"from\" field");
        return;
    }

    # Okay this part is a bit weird. The "from" could be a subquery
    my @database_names = $self->mongo->database_names();
    if (! grep { $_ eq $from } @database_names ){
        log_warn("Unknown \"from\" database \"$from\" in source for $db_name");
        return;
    }

    if (! $fields || ref $fields ne 'ARRAY' || @$fields < 1){
        log_warn("Source for $db_name is missing the \"fields\" field or it is not a non-empty array");
        return;
    }

    # Examine each field to make sure it makes sense. Since we're going to be turning
    # around and re-inserting this into the database the fields have to be renamed
    # so that they don't come back as "sum(values.input)" or something raw
    foreach my $field (@$fields){
        if ($field !~ / as \S+/){
            log_warn("Field \"$field\" for $db_name does not have a rename, must be \" as something\"");
            return;
        }
    }

    if (! defined $metadata || ref $metadata ne 'ARRAY' || @$metadata < 1){
        log_warn("Source for $db_name is missing the \"meta\" field or it is not a non-empty array");
        return;
    }

    log_debug("View information is correct");

    return 1;
}

sub stop(){
    my ( $self ) = @_;

    log_info( 'Stopping.' );

    exit(0);
}

1;
