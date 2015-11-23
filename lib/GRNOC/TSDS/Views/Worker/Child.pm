package GRNOC::TSDS::Views::Worker::Child;

use Moo;
use Types::Standard qw( Str Bool );

use Data::Dumper;

use GRNOC::Config;
use GRNOC::Log;
use GRNOC::WebService::Client;

use DateTime;
use DateTime::Format::Strptime;
use Parallel::ForkManager;
use Proc::Daemon;

use MongoDB;
use Net::AMQP::RabbitMQ;

use JSON::XS;
use Try::Tiny;

use constant QUEUE_FETCH_TIMEOUT => 10 * 1000;
use constant URN_PREFIX => "urn:publicid:IDN+grnoc.iu.edu";

extends 'GRNOC::TSDS::Views';

has is_running => ( is => 'rwp' );

has cloud => ( is => 'rwp' );

has wsc => ( is => 'rwp' );

sub start {
    my ( $self ) = @_;
    
    $self->_mongo_connect() or return;
    $self->_rabbit_connect() or return;
    $self->_wsc_connect() or return;

    # We're consuming in this one
    $self->rabbit->consume( 1, $self->rabbit_queue, {'no_ack' => 0} );

    log_debug("Entering main work loop");

    $self->_set_is_running( 1 );

    $self->_work_loop();
        
    return 1;
}


sub _work_loop {
    my ( $self ) = @_;

    while (1) {

        if (! $self->is_running ){
            return 0;
        }

        # receive the next rabbit message
        my $rabbit_message;
        
        try {
            $rabbit_message = $self->rabbit->recv( QUEUE_FETCH_TIMEOUT );
        }
        catch {            
            log_error( "Error receiving rabbit message: $_" );
            
            # reconnect to rabbit since we had a failure
            $self->_rabbit_connect();
        };

        # didn't get a message?
        if ( !$rabbit_message ) {            
            log_debug( 'No message received.' );
            
            # re-enter loop to retrieve the next message
            next;
        }
        
        # try to JSON decode the messages
        my $messages;
        
        try {            
            $messages = decode_json( $rabbit_message->{'body'} );
        }
        catch {            
            $self->logger->error( "Unable to JSON decode message: $_" );
        };

        if ( !$messages ) {
            
            try {                
                # reject the message and do NOT requeue it since its malformed JSON
                $self->rabbit->reject( 1, $rabbit_message->{'delivery_tag'}, 0 );
            }            
            catch {                
                log_error( "Unable to reject rabbit message: $_" );
                
                # reconnect to rabbit since we had a failure
                $self->_rabbit_connect();
            };
        }
        
        # retrieve the next message from rabbit if we couldn't decode this one
        next if ( !$messages );
        
        # make sure its an array (ref) of messages
        if ( ref( $messages ) ne 'ARRAY' ) {
            log_error( "Message body must be an array." );

            try {
                # reject the message and do NOT requeue since its not properly formed
                $self->rabbit->reject( 1, $rabbit_message->{'delivery_tag'}, 0 );
            }
            catch {
                log_error( "Unable to reject rabbit message: $_" );

                # reconnect to rabbit since we had a failure
                $self->_rabbit_connect();
            };

            next;        
        }

        my $num_messages = @$messages;
        log_debug( "Processing message containing $num_messages updates." );

        my $t1 = time();

        my $success = $self->_process_messages( $messages );

        my $t2 = time();
        my $delta = $t2 - $t1;

        log_debug( "Processed $num_messages updates in $delta seconds." );

        # didn't successfully consume the messages, so reject but requeue the entire message to try again
        if ( !$success ) {
            log_debug( "Rejecting rabbit message, requeueing." );

            try {
                $self->rabbit->reject( 1, $rabbit_message->{'delivery_tag'}, 1 );
            }
            catch {
                log_error( "Unable to reject rabbit message: $_" );

                # reconnect to rabbit since we had a failure
                $self->_rabbit_connect();
            };
        }
        # successfully consumed message, acknowledge it to rabbit
        else {
            log_debug( "Acknowledging successful message." );

            try {
                $self->rabbit->ack( 1, $rabbit_message->{'delivery_tag'} );
            }
            catch {
                log_error( "Unable to acknowledge rabbit message: $_" );

                # reconnect to rabbit since we had a failure
                $self->_rabbit_connect();
            };
        }            
    }
}

sub _wsc_connect {
    my ( $self ) = @_;

    my $cloud   = $self->config->get( '/config/tsds/@cloud' );
    my $user    = $self->config->get( '/config/tsds/@username' );
    my $pass    = $self->config->get( '/config/tsds/@password' );
    my $realm   = $self->config->get( '/config/tsds/@realm' );
  
    log_debug( "Creating WebService Client with user $user pass $pass realm $realm" );

    my $client = GRNOC::WebService::Client->new(uid     => $user,
                                                passwd  => $pass,
                                                realm   => $realm,
                                                usePost => 1,
                                                service_cache_file =>'/etc/grnoc/name-service-cacher/name-service.xml'
        );

    $self->_set_cloud($cloud);

    # Preflight check the URNs we need before we try to actually do anything
    if (! $client->set_service_identifier($self->_urn("TSDS:1:Push"))){
        log_warn("Error setting up client: " . $client->get_error());
        return;
    }
    if (! $client->set_service_identifier($self->_urn("TSDS:1:Query"))){
        log_warn("Error setting up client: " . $client->get_error());
        return;
    }

    log_debug("Client created");

    $self->_set_wsc($client);

    return 1;
}

sub _process_messages {
    my ( $self, $messages ) = @_;

    foreach my $message (@$messages){
        $self->_validate_view($message) or next;

        my $to       = $message->{'to'};
        my $interval = $message->{'interval'};
        my $from     = $message->{'from'};
        my $fields   = $message->{'fields'};
        my $where    = $message->{'where'};
        my $by       = $message->{'by'};
        my $having   = $message->{'having'};
        my $order    = $message->{'order'};
        my $limit    = $message->{'limit'};
        my $offset   = $message->{'offset'};
        my $start    = $message->{'start'};
        my $end      = $message->{'end'};
        my $meta     = $message->{'meta'};

        # Convert start and end to proper representation
        my $date_format = DateTime::Format::Strptime->new( pattern => '%m/%d/%Y %H:%M:%S');
        my $start_time  = $date_format->format_datetime(DateTime->from_epoch( epoch => $start ));
        my $end_time    = $date_format->format_datetime(DateTime->from_epoch( epoch => $end ));


        # Need to build query and execute
        my $query = "get ";
        $query .= join(", ", @$fields);
        $query .= " between(\"$start_time\", \"$end_time\") ";
        $query .= " by " . join(", ", @$by);
        $query .= " from $from ";
        $query .= " where $where ";

        if ($having){
            $query .= " having $having ";
        }
        if (defined $limit){
            $query .= " limit $limit offset $offset ";
        }
        if (defined $order){
            $query .= " ordered by " . join(", ", @$order);
        }

        log_debug("Final query: $query");

        $self->wsc->set_service_identifier($self->_urn("TSDS:1:Query"));

        my $response = $self->wsc->query(query => $query);

        if (! defined $response){
            log_warn("Didn't get response for query $query, error was: " . Dumper($self->wsc->get_error()));
            return;
        }

        if ($response->{'error'}){
            my $text = $response->{'error_text'};
            log_warn("Server returned error for query $query\nError was: $text");

            # If it's a syntax error or an error about running unconstrained queries, this query
            # will never succeed so don't requeue the message
            next if ($text =~ /syntax/ || $text =~ /unconstrained/);

            # If it's another type of error we should probably try again so requeue
            return;
        }

        # Okay we have our response from the query, now time to build up 
        # the writer message to submit this as data
        my $results = $response->{'results'};

        log_debug("Query results: " . Dumper($results));

        foreach my $row (@$results){
            my $writer_message = {
                interval => $interval,
                time     => $start,
                type     => $to,
                values   => {},
                meta     => {}
            };
           
            foreach my $meta (@$meta){
                $writer_message->{'meta'}{$meta} = delete $row->{$meta};
            }
            
            foreach my $val_key (keys %$row){
                $writer_message->{'values'}{$val_key} = $row->{$val_key};
            }

            log_debug("Writer message: " . Dumper($writer_message));

            $self->wsc->set_service_identifier($self->_urn("TSDS:1:Push"));

            $response = $self->wsc->add_data(data => encode_json([$writer_message]));

            warn Dumper($response);
            
        }
    }

    return 1;
}

sub stop {
    my ( $self ) = @_;
    
    $self->_set_is_running( 0 );

    log_info( 'Stopping child views process $$.' );
}

sub _urn {
    my ( $self, $service ) = @_;

    return URN_PREFIX . ":" . $self->cloud . ":" . $service;
}

1;
