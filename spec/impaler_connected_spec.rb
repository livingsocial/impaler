require 'impaler'

# These tests cover the Impaler interaction with a real impala and hive server.
# To run these define environment variables for IMPALA_SERVER and HIVETHRIFT_SERVER
# in the format "server:port".

IMPALA_SERVER=ENV['IMPALA_SERVER']
HIVETHRIFT_SERVER=ENV['HIVETHRIFT_SERVER']
has_servers=!IMPALA_SERVER.nil? && !HIVETHRIFT_SERVER.nil?
IMPALA_SERVERS=[IMPALA_SERVER]
HIVETHRIFT_SERVERS=[HIVETHRIFT_SERVER]

TEST_TABLE=ENV['TEST_TABLE']
TEST_TABLE_COLUMN=ENV['TEST_TABLE_COLUMN']
has_tables=!TEST_TABLE.nil? && !TEST_TABLE_COLUMN.nil?

run_tests=has_tables && has_servers

SKIP_SLOW=!ENV['SKIP_SLOW'].nil?

def connect
  Impaler.connect(IMPALA_SERVERS, HIVETHRIFT_SERVERS)
end

def connect_impala
  Impaler.connect(IMPALA_SERVERS, nil)
end

def connect_hivethrift
  Impaler.connect(nil, HIVETHRIFT_SERVERS)
end


describe Impaler, :if => run_tests do 
  it("Connected skip", :if=>!run_tests) {
    puts "Skipping connected tests for Impaler, set the environment variables IMPALA_SERVER, HIVETHRIFT_SERVER, TEST_TABLE, and TEST_TABLE_COLUMN to enable these"
    puts "IMPALA_SERVER and HIVETHRIFT_SERVER are in the format 'server:port'"
    puts "TEST_TABLE should be a fairly small table for quick tests and TEST_TABLE_COLUMN will be used for some simple test queries where Impala is known to not support the query"
    puts "Optionally set the environment varialbe SKIP_SLOW=true to skip the hive invocations which are a bit slow"
  }


  describe "connect" do
    it "connects without error" do
      expect { Impaler::connect(IMPALA_SERVERS, HIVETHRIFT_SERVERS) }.not_to raise_error
      expect { Impaler::connect(IMPALA_SERVERS, nil) }.not_to raise_error
      expect { Impaler::connect(nil, HIVETHRIFT_SERVERS) }.not_to raise_error
    end

    it "connects with single value server entries without error" do
      expect { Impaler::connect(IMPALA_SERVER, HIVETHRIFT_SERVER) }.not_to raise_error
      expect { Impaler::connect(IMPALA_SERVER, nil) }.not_to raise_error
      expect { Impaler::connect(nil, HIVETHRIFT_SERVER) }.not_to raise_error
    end
  end

  describe "simple query" do
    it "supports a count(*) query" do
      c = connect
      count = (c.query "select count(*) c from #{TEST_TABLE}").first[:c]
      (c.query "select count(*) c from #{TEST_TABLE}").first[:c].should eq(count)
      (c.query "select count(*) c from #{TEST_TABLE}", Impaler::IMPALA_ONLY).first[:c].should eq(count)
      (c.query "select count(*) c from #{TEST_TABLE}", Impaler::IMPALA_THEN_HIVE).first[:c].should eq(count)
      if !SKIP_SLOW 
        (c.query "select count(*) c from #{TEST_TABLE}", Impaler::HIVE_ONLY).first[:c].should eq(count)
      end
    end

    it "fails with garbage queries" do
      c = connect
      expect { c.query "select sdffdsa from lkjasdfjkhadf", Impaler::IMPALA_ONLY }.to raise_error(Impala::Protocol::Beeswax::BeeswaxException)
      expect { c.query "select sdffdsa from lkjasdfjkhadf", Impaler::HIVE_ONLY }.to raise_error(HiveServerException)
      expect { c.query "select sdffdsa from lkjasdfjkhadf" }.to raise_error(HiveServerException)
    end

  end

  describe "unsupported impala queries", :unless => SKIP_SLOW do

    it "fails when run with impala only" do
      c = connect
      expect { c.query "select collect_set(#{TEST_TABLE_COLUMN}) from #{TEST_TABLE}", Impaler::IMPALA_ONLY }.to raise_error(Impala::Protocol::Beeswax::BeeswaxException)
    end

    it "falls back to hive if impala generates an error" do
      c = connect
      expect { c.query "select collect_set(#{TEST_TABLE_COLUMN}) from #{TEST_TABLE}" }.not_to raise_error
    end

  end

  describe "handles down servers" do
    it "handles having no impala server" do
      c = connect_hivethrift
      expect { c.query "select count(*) c from #{TEST_TABLE}", Impaler::IMPALA_ONLY }.to raise_error(Impaler::QueryError)
      if !SKIP_SLOW 
        expect { c.query "select count(*) c from #{TEST_TABLE}", Impaler::HIVE_ONLY }.not_to raise_error
        expect { c.query "select count(*) c from #{TEST_TABLE}", Impaler::IMPALA_THEN_HIVE }.not_to raise_error
      end
    end

    it "handles having no hive server" do
      c = connect_impala
      expect { c.query "select count(*) c from #{TEST_TABLE}", Impaler::HIVE_ONLY }.to raise_error(Impaler::QueryError)
      expect { c.query "select count(*) c from #{TEST_TABLE}", Impaler::IMPALA_ONLY }.not_to raise_error
      expect { c.query "select count(*) c from #{TEST_TABLE}", Impaler::IMPALA_THEN_HIVE }.not_to raise_error
    end
  end

  describe "query consistency", :unless => SKIP_SLOW do

    it "queries return the same regardless of connection type" do
      q="select * from #{TEST_TABLE} limit 5"
      c = connect
      base = c.query(q)

      t=connect_impala.query(q)
      expect(t).to eq(base)

      t=connect_hivethrift.query(q)
      expect(t).to eq(base)
    end
  end


  describe "columns method works" do

    it "columns returns the same regardless of connection type" do
      c = connect
      base = c.columns("#{TEST_TABLE}")

      t=connect_impala.columns("#{TEST_TABLE}")
      expect(t).to eq(base)

      t=connect_hivethrift.columns("#{TEST_TABLE}")
      expect(t).to eq(base)
    end
  end


  describe "row_count method works", :unless => SKIP_SLOW do

    it "row_count returns the same regardless of connection type" do
      c = connect
      base = c.row_count("#{TEST_TABLE}")

      t=connect_impala.row_count("#{TEST_TABLE}")
      expect(t).to eq(base)

      t=connect_hivethrift.row_count("#{TEST_TABLE}")
      expect(t).to eq(base)
    end
  end

end
