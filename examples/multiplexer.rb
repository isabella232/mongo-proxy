require 'mongo-proxy'
require 'yaml'

config = {
    :client_port => 27017,
    :server_port => 27000,
    :read_only => true,
}

mappings = YAML.load_file('multiplexer.yaml')

list_db_request = nil

proxy = MongoProxy.new(config)

proxy.add_callback_to_back do |conn, msg|

    database = msg[:database]
    collection = msg[:collection]
    query = msg[:query]

    if database == 'admin' and query
        if query.has_key? 'listDatabases'
            list_db_request = msg[:header][:requestID]
        end
    end

    if database != 'admin' and collection != '$cmd'
        conn.server(:srv, {
          :host => mappings[database]['server_host'],
          :port => mappings[database]['server_port']})
    end

    msg
end

proxy.add_callback_to_server_response do |conn, msg|

    database = msg[:database]
    collection = msg[:collection]
    response_to = msg[:header][:responseTo]

    if response_to == list_db_request
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
