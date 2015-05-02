namespace :db do

  desc "import from db"
  task :import, [:user, :password] => []  do |t, args|
    Bundler.require(:default)

    store = Metermaid::ActiveRecordStore.new
    store.open!

    result = Metermaid::PGandE::Fetcher.scrape!(
      username: args[:user],
      password: args[:password],
      start_date: 1.month.ago,
      end_date: Time.now,
    )
    result.each do |row|
      Metermaid::DB::MetermaidSample.where(row).first_or_create
    end

    store.close!
  end

end
