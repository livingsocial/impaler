require "rbhive"
require "impala"

module Impaler

  IMPALA_THEN_HIVE = 1
  HIVE_ONLY = 2
  IMPALA_ONLY = 3

  class Manager

    def initialize(impala_servers, hivethrift_servers, logger=Logger.new(STDOUT))
      if impala_servers.nil? and hivethrift_servers.nil? then
        raise Impaler::ConnectionError.new("No impaler or hive servers were specified, at least one is required")
      end
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


    def query(sql, query_mode = Impaler::IMPALA_THEN_HIVE)
      ret = nil
      error = nil
      success = false
      unless query_mode == Impaler::HIVE_ONLY then
        begin
          @logger.debug "Trying query in impala"
          ret = @impala_connection.query(sql)
          @logger.debug "Successful query in impala"
          success = true
        rescue StandardError => e
          error = e
          @logger.warn "Impala error: #{e}"
        end
      end

      unless success || query_mode == Impaler::IMPALA_ONLY then
        begin
          @logger.debug "Trying query in hive"
          ret = @hivethrift_connection.fetch(sql)
          @logger.debug "Successful query in hive"
          success = true
        rescue StandardError => e
          error = e
          @logger.warn "Hive error: #{e}"
        end
      end

      if !success && !error.nil? then
        throw error
      end
      return ret
    end

  end

end
