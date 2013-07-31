require 'impaler'

# These tests cover the internals of the Impaler library in isolation as much as possible

describe Impaler do

  describe "connect" do
    it "fails with no servers" do
      expect { Impaler::connect(nil, nil) }.to raise_error(Impaler::ConnectionError)
      expect { Impaler::connect() }.to raise_error(Impaler::ConnectionError)
      expect { Impaler::connect(impala_servers=nil) }.to raise_error(Impaler::ConnectionError)
      expect { Impaler::connect(hivethrift_servers=nil) }.to raise_error(Impaler::ConnectionError)
      expect { Impaler::connect(impala_servers=nil, hivethrift_servers=nil) }.to raise_error(Impaler::ConnectionError)
    end
  end
end
