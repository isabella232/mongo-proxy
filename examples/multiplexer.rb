require 'mongo-proxy'
require 'yaml'

config = {
    :client_port => 27017,
    :server_port => 27000,
    :read_only => true,
}

mappings = YAML.load_file('multiplexer.yaml')

auth_requests = {}
dbs_requests = {}

proxy = MongoProxy.new(config)

# Callback prior to user authentication
proxy.add_callback_to_front do |conn, msg|

    database = msg[:database]
    collection = msg[:collection]
    query = msg[:query]

    if database != 'admin' and collection == '$cmd' and query
        if query.has_key? 'authenticate'
            auth_requests[conn] = {
                :requestID => msg[:header][:requestID],
                :user => query['user'],
            }
        end
    end

    msg
end


# Callback after successful authentication
proxy.add_callback_to_back do |conn, msg|

    database = msg[:database]
    collection = msg[:collection]
    query = msg[:query]

    if database == 'admin' and query
        if query.has_key? 'listDatabases'
            dbs_requests[conn] = {
                :requestID => msg[:header][:requestID]
            }
        end
    end

    if mappings.has_key? database
        conn.server(:srv, {
          :host => mappings[database]['server_host'],
          :port => mappings[database]['server_port']})

        if auth_requests.has_key? conn and query
            msg[:query] = {
                '$query' => query,
                '$comment' => auth_requests[conn][:user],
            }
        end
    end

    msg
end


# Callback on server query response
proxy.add_callback_to_server_response do |conn, msg|

    database = msg[:database]
    collection = msg[:collection]
    response_to = msg[:header][:responseTo]

    auth_request = auth_requests[conn]
    if auth_request and response_to == auth_request[:requestID]
        msg[:documents] = [{
            'dbname' => database,
            'user' => auth_request[:user],
            'ok' => 1.0,
        }]
    end

    dbs_request = dbs_requests[conn]
    if dbs_request and response_to == dbs_request[:requestID]
        msg[:documents] = {
          :databases => mappings.keys.collect{|db_name| {
            :name => db_name,
            :sizeOnDisk => 0,
            :empty => true,
          }},
          :totalSize => 0,
          :ok => 1,
        }
    end

    msg
end

proxy.start
