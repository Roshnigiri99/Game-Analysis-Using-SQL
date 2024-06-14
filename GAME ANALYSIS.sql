CREATE DATABASE game_analysis;
USE game_analysis;
CREATE TABLE pd (
    myunknowncolumn INT,
    P_ID INT ,
    PName VARCHAR(255),
    L1_Status INT,
    L2_Status INT,
    L1_Code VARCHAR(50),
    L2_Code VARCHAR(50)
);

LOAD DATA INFILE 'C:/ProgramData/MYSQL/MYSQL Server 8.0/Uploads/player_details.csv'
INTO TABLE pd
FIELDS terminated by ','
LINES terminated by '\n'
IGNORE 1 ROWS;
SELECT * FROM pd;

alter table pd modify L1_Status varchar(30);
alter table pd modify L2_Status varchar(30);
alter table pd modify P_ID int primary key;
alter table pd drop myunknowncolumn;

CREATE TABLE ld (
MyUnknowncolumn INT,
P_ID INT,
Dev_ID VARCHAR (100),
TimeStamp DATETIME,
Stages_crossed INT,
Level INT,
Difficulty VARCHAR (10),
Kill_Count INT,
Headshots_Count INT,
Score INT,
Lives_Earned INT
);

LOAD DATA INFILE 'C:/ProgramData/MYSQL/MYSQL Server 8.0/Uploads/level_details2.csv'
INTO TABLE ld
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@MyUnknowncolumn, @P_ID, @Dev_ID, @TimeStamp, @Stages_crossed, @Level, @Difficulty, @Kill_Count, @Headshots_Count, @Score, @Lives_Earned)
SET
    MyUnknowncolumn = @MyUnknowncolumn,
    P_ID = @P_ID,
    Dev_ID = @Dev_ID,
    TimeStamp = STR_TO_DATE(@TimeStamp, '%d-%m-%Y %H:%i:%s'),
    Stages_crossed = @Stages_crossed,
    Level = @Level,
    Difficulty = @Difficulty,
    Kill_Count = @Kill_Count,
    Headshots_Count = @Headshots_Count,
    Score = @Score,
    Lives_Earned = @Lives_Earned;
    
SELECT * FROM ld;

alter table ld drop MyUnknowncolumn;
alter table ld change timestamp start_datetime datetime;
alter table ld modify Dev_Id varchar(10);
alter table ld modify Difficulty varchar(15);
alter table ld add primary key(P_ID,Dev_id,start_datetime);

-- Problem Statement - Game Analysis dataset
-- 1) Players play a game divided into 3-levels (L0,L1 and L2)
-- 2) Each level has 3 difficulty levels (Low,Medium,High)
-- 3) At each level,players have to kill the opponents using guns/physical fight
-- 4) Each level has multiple stages at each difficulty level.
-- 5) A player can only play L1 using its system generated L1_code.
-- 6) Only players who have played Level1 can possibly play Level2 
--    using its system generated L2_code.
-- 7) By default a player can play L0.
-- 8) Each player can login to the game using a Dev_ID.
-- 9) Players can earn extra lives at each stage in a level.

-- pd (P_ID,PName,L1_status,L2_Status,L1_code,L2_Code)
-- ld (P_ID,Dev_ID,start_time,stages_crossed,level,difficulty,kill_count,
-- headshots_count,score,lives_earned)


-- Extract P_ID,Dev_ID,PName and Difficulty_level of all players at level 0

SELECT ld.P_ID, pd.PName, ld.Dev_ID, ld.Difficulty
FROM ld
JOIN pd ON ld.P_ID = pd.P_ID
WHERE Level = 0;

-- Q2) Find Level1_code wise Avg_Kill_Count where lives_earned is 2 and atleast 3 stages are crossed


SELECT pd.L1_Code, AVG(ld.Kill_Count) AS Avg_Kill_Count
FROM ld
INNER JOIN pd ON ld.P_ID = pd.P_ID
WHERE Lives_Earned = 2
AND Stages_crossed >= 3
GROUP BY L1_Code;

-- Q3) Find the total number of stages crossed at each difficulty level where for Level2 with 
-- players use zm_series devices. Arrange the result in decsreasing order of total number 
-- of stages crossed.

SELECT SUM(Stages_crossed) AS total_stages_crossed,Difficulty
FROM ld 
WHERE Level =2
AND Dev_ID like 'zm%'
GROUP BY Difficulty
ORDER BY total_stages_crossed DESC;



-- Q4) Extract P_ID and the total number of unique dates for those players 
-- who have played games on multiple days.

SELECT
    P_ID,
    COUNT(DISTINCT DATE(start_datetime)) AS Total_Unique_Dates
FROM ld
GROUP BY P_ID
HAVING
    COUNT(DISTINCT DATE(start_datetime)) > 1;

-- Q5) Find P_ID and level wise sum of kill_counts where kill_count
-- is greater than avg kill count for the Medium difficulty.
SELECT P_ID, Level,
    SUM(Kill_Count) 
FROM ld
WHERE Kill_Count >(SELECT AVG(Kill_Count) FROM ld 
WHERE Difficulty = 'Medium')
GROUP BY P_ID, Level; 

-- Q6)  Find Level and its corresponding Level code wise sum of lives earned 
-- excluding level 0. Arrange in asecending order of level.

SELECT 
    ld.Level, 
    pd.L1_Code,
    pd.L2_Code,
    SUM(ld.Lives_Earned) AS Total_Lives_Earned
FROM 
    ld
JOIN 
    pd ON ld.P_ID = pd.P_ID
WHERE 
    ld.Level != 0
GROUP BY 
    ld.Level, 
    pd.L1_Code,
    pd.L2_Code
ORDER BY 
    ld.Level ASC;


-- Q7) Find Top 3 score based on each dev_id and Rank them in increasing order
-- using Row_Number. Display difficulty as well. 

SELECT Dev_ID, Difficulty,Score,
Ranked
FROM (
    SELECT Dev_ID,Difficulty,Score,
        ROW_NUMBER() OVER (PARTITION BY Dev_ID ORDER BY Score DESC) 
        AS Ranked
    FROM 
        ld
) AS RankedScores
WHERE 
    Ranked <= 3;

-- Q8) Find first_login datetime for each device id

SELECT Dev_ID ,min(start_datetime) 
FROM ld 
GROUP BY Dev_ID;
-- Q9) Find Top 5 score based on each difficulty level and Rank them in 
-- increasing order using Rank. Display dev_id as well.

WITH RankedScores AS (
    SELECT 
        Dev_ID,
        Difficulty,
        Score,
        RANK() OVER (PARTITION BY Difficulty ORDER BY Score DESC) AS Ranked FROM ld)
SELECT Dev_ID,Difficulty,Score,
    Ranked FROM RankedScores
WHERE Ranked <= 5;


-- Q10) Find the device ID that is first logged in(based on start_datetime) 
-- for each player(p_id). Output should contain player id, device id and 
-- first login datetime.

SELECT P_ID,Dev_ID,
        MIN(start_datetime) AS First_Login_DateTime
FROM ld
GROUP BY P_ID, Dev_ID;


-- Q11) For each player and date, how many kill_count played so far by the player. 
-- That is, the total number of games played by the player until that date.
-- a) window function

SELECT
P_ID,
    CAST(start_datetime AS DATE) AS'Date',
    SUM(Kill_Count) OVER(PARTITION BY P_ID ORDER BY start_datetime) AS Total_Kill_count
    FROM ld;
    
-- b) without window function 
SELECT P_ID,
CAST(start_datetime AS DATE) AS 'Date',
(SELECT SUM(Kill_Count) 
FROM ld ld2 WHERE ld2.P_ID = ld.P_ID 
AND ld2.start_datetime <= ld.start_datetime)
AS Total_Kill_count
    FROM ld;

-- Q12) Find the cumulative sum of an stages crossed over a start_datetime 
-- for each player id but exclude the most recent start_datetime

SELECT P_ID, start_datetime,
SUM(Stages_Crossed)OVER(PARTITION BY P_ID ORDER BY start_datetime ASC) 
AS cumulative_sum_of_stages_crossed
FROM ld;

-- Q13) Extract top 3 highest sum of score for each device id and the corresponding player_id

SELECT Dev_ID, P_ID, Score
FROM (
    SELECT 
        P_ID,
        Dev_ID,
        SUM(Score) AS Score,
        ROW_NUMBER() OVER (PARTITION BY Dev_ID ORDER BY SUM(Score) DESC) AS Ranked
    FROM 
        ld
    GROUP BY 
        Dev_ID, P_ID
) AS ranked_scores
WHERE 
    Ranked <= 3;

-- Q14) Find players who scored more than 50% of the avg score scored by sum of 
-- scores for each player_id

SELECT
    P_ID,
    SUM(Score) AS Total_Score
    FROM ld 
GROUP BY P_ID
HAVING
    SUM(Score) > 0.5 * (SELECT AVG(Score) 
    FROM ld)


-- Q15) Create a stored procedure to find top n headshots_count based on each dev_id and Rank them in increasing order
-- using Row_Number. Display difficulty as well.

DELIMITER //
CREATE PROCEDURE TOPN(
    IN n INT
)
BEGIN
    SELECT Dev_ID,Difficulty,Headshots_Count
    FROM(
    SELECT Dev_ID,Difficulty,Headshots_Count,
        ROW_NUMBER() OVER (PARTITION BY Dev_ID ORDER BY Headshots_Count ) AS Ranked
    FROM ld
    )AS task
    WHERE Ranked <=n;
END//

DELIMITER ;
call TopN(6)


























