require "rbhive"
require "impala"

module Impaler

  class Manager

    def initialize(impala_servers, hivethrift_servers, logger=Logger.new(STDOUT))
      @impala_servers = impala_servers
      @hivethrift_servers = hivethrift_servers
      @logger = logger
      open
    end

    def open
      impala_server = @impala_servers.choice.split(":")
      impala_host = impala_server[0]
      impala_port = impala_server[1]
      @logger.debug "Impala connection #{impala_host}:#{impala_port}"
      @impala_connection = Impala.connect(impala_host, impala_port)
      @impala_connection.open
      hivethrift_server = @hivethrift_servers.choice.split(":")
      hivethrift_host = hivethrift_server[0]
      hivethrift_port = hivethrift_server[1]
      @logger.debug "Hivethrift connection #{hivethrift_host}:#{hivethrift_port}"
      @hivethrift_connection = RBHive::Connection.new(hivethrift_host, hivethrift_port)
      @hivethrift_connection.open
    end


    def query(sql)
      ret = nil
      begin
        @logger.debug "Trying query in impala"
        ret = @impala_connection.query(sql)
        @logger.debug "Successful query in impala"
      rescue StandardError => e
        @logger.warn "Impala error: #{e}"
        @logger.debug "Impala failed, falling back to hive"
        ret = @hivethrift_connection.fetch(sql)
        @logger.debug "Successful query in hive"
      end
      return ret
    end

  end

end
