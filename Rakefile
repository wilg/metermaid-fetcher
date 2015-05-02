namespace :db do

  desc "destroy database tables"
  task :destroy do |t|
    Bundler.require(:default)

    store = Metermaid::ActiveRecordStore.new
    store.open!
    store.reset!
    store.close!
  end

  desc "import from db"
  task :import do |t|
    Bundler.require(:default)

    store = Metermaid::ActiveRecordStore.new
    store.open!

    # CREDENTIALS=username,password;username2;password2
    ENV["CREDENTIALS"].split(";").each do |credentials|

      args = credentials.split(",")

      username = args[0]
      password = args[1]

      # Dump other pairs into JSON in the database! Yay Postgres!
      args.shift(2)
      other_args = args.each_slice(2).to_h

      result = Metermaid::PGandE::Fetcher.scrape!(
        username: username,
        password: password,
        start_date: 1.month.ago,
        end_date: 2.days.from_now,
        additional_metadata: other_args.presence
      )
      result.each do |row|
        metadata = row.delete(:additional_metadata)
        sample = Metermaid::DB::Sample.where(row).first_or_initialize
        if sample.new_record?
          sample.additional_metadata = metadata
          sample.save
        end
      end

    end

    store.close!

  end

end
