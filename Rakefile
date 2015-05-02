namespace :db do

  desc "import from db"
  task :import do |t|
    Bundler.require(:default)

    store = Metermaid::ActiveRecordStore.new
    store.open!

    # CREDENTIALS=username,password;username2;password2
    ENV["CREDENTIALS"].split(";").each do |credentials|

      (username, password) = credentials.split(",")

      result = Metermaid::PGandE::Fetcher.scrape!(
        username: username,
        password: password,
        start_date: 1.month.ago,
        end_date: Time.now,
      )
      result.each do |row|
        Metermaid::DB::Sample.where(row).first_or_create
      end

    end

    store.close!

  end

end
