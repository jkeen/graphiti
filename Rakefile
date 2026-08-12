require "bundler/gem_tasks"
require "rspec/core/rake_task"
require "appraisal"
# Standard is silent on success; show the inspected-files summary.
ENV["STANDARDOPTS"] ||= "--format progress"
require "standard/rake"

RSpec::Core::RakeTask.new(:spec) do |t|
  if ENV["APPRAISAL_INITIALIZED"]
    t.pattern = "spec/integration/rails"
  end
end

namespace :performance do
  desc "Rewrite spec/performance/baselines.yml from the current working tree"
  task :baseline do
    sh "PERF_BASELINE=write bundle exec rspec spec/performance"
    puts "\nreview the diff before committing: a raised number is a regression you are accepting"
  end

  desc "Check allocations against the committed baselines"
  task :check do
    sh "bundle exec rspec spec/performance"
  end
end

if ENV["APPRAISAL_INITIALIZED"]
  task default: [:spec]
else
  task default: [:standard, :spec, :appraisal]
end
