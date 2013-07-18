# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'impaler/version'

Gem::Specification.new do |spec|
  spec.name          = "impaler"
  spec.version       = Impaler::VERSION
  spec.authors       = ["John Meagher","Trent Albright"]
  spec.email         = ["john.meagher@gmail.com","trent.albright@gmail.com"]
  spec.description   = %q{Wrapper around Impala and Hive gems}
  spec.summary       = %q{Run in Impala when possible and fall back to Hive when needed}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"
end
