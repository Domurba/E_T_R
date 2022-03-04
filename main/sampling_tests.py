import pandas as pd


def reader(num_rows):
    return pd.read_csv(
        "../DATA/usage.csv",
        names=[
            "Customer ID",
            "Event Start Time",
            "Event Type",
            "Rate Plan ID",
            "Billing Flag 1",
            "Billing Flag 2",
            "Duration",
            "Charge",
            "Month",
        ],
        usecols=[
            "Customer ID",
            "Event Start Time",
            "Event Type",
            "Rate Plan ID",
            "Billing Flag 1",
            "Billing Flag 2",
            "Duration",
            "Charge",
        ],
        parse_dates=["Event Start Time"],
        nrows=num_rows,
    )


def billing_flags(n=500):
    """
    Checks for any null values in csv file
    [n = None] for full scan.
    """
    df = reader(num_rows=n)
    return df[
        (df["Billing Flag 1"] == 0) & (df["Billing Flag 2"] == 0) & (df["Charge"] == 0)
    ].to_csv("sample_charge_flags", index=False)


def check_null(n=500):
    """
    Checks for any null values in csv file
    [n = None] for full scan.
    NO NULL VALUES FOUND
    """
    df = reader(num_rows=n)
    return df.isnull().values.any()


def unique_event_types(n=500):
    """
    Retrieves the unique event types.
    [n = None] for full scan.
    unique_vals = ["DATA", "VOICE", "SMS", "MMS"]
    """
    df = pd.read_csv(
        "../DATA/usage.csv",
        names=[
            "Customer ID",
            "Event Start Time",
            "Event Type",
            "Rate Plan ID",
            "Billing Flag 1",
            "Billing Flag 2",
            "Duration",
            "Charge",
            "Month",
        ],
        usecols=["Event Type"],
        nrows=n,
    )
    return df["Event Type"].unique()


def count_unique_plans(n=500):
    """
    Retrieves the count of unique plan id's.
    [n = None] for full scan.
    unique_vals = 2333
    """
    df = pd.read_csv(
        "../DATA/usage.csv",
        names=[
            "Customer ID",
            "Event Start Time",
            "Event Type",
            "Rate Plan ID",
            "Billing Flag 1",
            "Billing Flag 2",
            "Duration",
            "Charge",
            "Month",
        ],
        usecols=[
            "Rate Plan ID",
        ],
        nrows=n,
    )
    return df["Rate Plan ID"].nunique()
