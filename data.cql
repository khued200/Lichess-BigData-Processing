-- Create a keyspace
CREATE KEYSPACE lichess WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Create a table
CREATE TABLE IF NOT EXISTS lichess.game (
    Event text,
    Site text,
    Date text,
    Round text,
    White text,
    Black text,
    Result text,
    UTCDate text,
    UTCTime text,
    WhiteElo text,
    BlackElo text,
    WhiteRatingDiff text,
    BlackRatingDiff text,
    TimeControl text,
    Termination text,
    Variant text,
    Moves text,
    PRIMARY KEY (Event, Date)
);
