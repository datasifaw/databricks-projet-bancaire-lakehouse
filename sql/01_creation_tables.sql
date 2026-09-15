CREATE DATABASE Banque_Databricks;
GO

USE Banque_Databricks;
GO

    



IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'banking')
BEGIN
    EXEC('CREATE SCHEMA banking');
END
GO

CREATE TABLE banking.branches (
    branch_code VARCHAR(10) NOT NULL,
    branch_name VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    region VARCHAR(100) NULL,
    created_at DATETIME2 DEFAULT SYSDATETIME(),
    CONSTRAINT PK_branches PRIMARY KEY (branch_code)
);
GO

CREATE TABLE banking.customers (
    customer_id INT NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    date_of_birth DATE NOT NULL,
    pan_number VARCHAR(20) NULL,
    email VARCHAR(150) NULL,
    phone_number VARCHAR(20) NULL,
    kyc_status VARCHAR(50) DEFAULT 'PENDING',
    branch_code VARCHAR(10) NOT NULL,
    created_at DATETIME2 DEFAULT SYSDATETIME(),
    updated_at DATETIME2 DEFAULT SYSDATETIME(),
    CONSTRAINT PK_customers PRIMARY KEY (customer_id),
    CONSTRAINT FK_customers_branches
        FOREIGN KEY (branch_code)
        REFERENCES banking.branches(branch_code)
);
GO

CREATE TABLE banking.accounts (
    account_id BIGINT NOT NULL,
    customer_id INT NOT NULL,
    account_type VARCHAR(50) NOT NULL,
    balance DECIMAL(18,2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'INR',
    branch_code VARCHAR(10) NOT NULL,
    status VARCHAR(50) DEFAULT 'ACTIVE',
    opened_date DATE NOT NULL,
    created_at DATETIME2 DEFAULT SYSDATETIME(),
    updated_at DATETIME2 DEFAULT SYSDATETIME(),
    CONSTRAINT PK_accounts PRIMARY KEY (account_id),
    CONSTRAINT FK_accounts_customers
        FOREIGN KEY (customer_id)
        REFERENCES banking.customers(customer_id),
    CONSTRAINT FK_accounts_branches
        FOREIGN KEY (branch_code)
        REFERENCES banking.branches(branch_code)
);
GO

CREATE TABLE banking.transactions (
    txn_id BIGINT NOT NULL,
    account_id BIGINT NOT NULL,
    txn_type VARCHAR(20) NOT NULL,
    amount DECIMAL(18,2) NOT NULL,
    txn_timestamp DATETIME2 NOT NULL,
    channel VARCHAR(50) NULL,
    status VARCHAR(50) DEFAULT 'SUCCESS',
    created_at DATETIME2 DEFAULT SYSDATETIME(),
    CONSTRAINT PK_transactions PRIMARY KEY (txn_id),
    CONSTRAINT FK_transactions_accounts
        FOREIGN KEY (account_id)
        REFERENCES banking.accounts(account_id)
);
GO
