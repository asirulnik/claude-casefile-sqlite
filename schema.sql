-- Schema definition for the Case File and Billing Management Database

-- Clients table
CREATE TABLE IF NOT EXISTS clients (
    client_id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_name TEXT NOT NULL,
    contact_info TEXT
);

-- Case Files table
CREATE TABLE IF NOT EXISTS case_files (
    case_id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER,
    case_name TEXT,
    case_status TEXT,
    FOREIGN KEY (client_id) REFERENCES clients(client_id)
);

-- Case File Entries table
CREATE TABLE IF NOT EXISTS case_file_entries (
    entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id INTEGER,
    type TEXT,
    date DATETIME,
    title TEXT,
    from_party TEXT,
    to_party TEXT,
    cc_party TEXT,
    content TEXT,
    attachments TEXT,
    synopsis TEXT,
    comments TEXT,
    billing_start DATETIME,
    billing_stop DATETIME,
    billing_hrs REAL,
    FOREIGN KEY (case_id) REFERENCES case_files(case_id)
);

-- Billing Entries table
CREATE TABLE IF NOT EXISTS billing_entries (
    billing_id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id INTEGER NOT NULL,
    entry_id INTEGER,
    billing_category TEXT,
    billing_start DATETIME,
    billing_stop DATETIME,
    billing_hours REAL,
    billing_description TEXT,
    FOREIGN KEY (case_id) REFERENCES case_files(case_id),
    FOREIGN KEY (entry_id) REFERENCES case_file_entries(entry_id)
);