CREATE TABLE IF NOT EXISTS ico_data (
  ico_name TEXT PRIMARY KEY NOT NULL,
  description TEXT,
  ico_url TEXT,
  ico_team TEXT,
  pre_ico BOOLEAN DEFAULT FALSE,
  financials TEXT,
  countries TEXT,
  location TEXT
);

CREATE TABLE IF NOT EXISTS investor_data (
  member_name TEXT PRIMARY KEY NOT NULL,
  ico_name TEXT NOT NULL,
  origin_url TEXT,
  profile_pic BLOB,
  member_title TEXT,
  member_tags TEXT,
  soc_linkedin TEXT,
  soc_twitter TEXT,
  soc_facebook TEXT,
  soc_github TEXT,
  soc_dribble TEXT,
  soc_behance TEXT,
  soc_blog TEXT,
  soc_personal_site TEXT,
  member_experience TEXT,
  member_location TEXT,
  member_skills TEXT,
  member_investments TEXT,
  is_founder BOOLEAN DEFAULT FALSE, -- Tells us that this employee is listed as a founder.
      FOREIGN KEY (ico_name) REFERENCES ico_data(ico_name)
);

CREATE TABLE IF NOT EXISTS proxies (
  proxy TEXT PRIMARY KEY NOT NULL,
  proxy_type TEXT,
  is_enabled INTEGER DEFAULT 1,
  country TEXT
);