
CREATE TABLE EMPLOYEESAMPLE (
    id INT PRIMARY KEY,
    name STRING
);
DROP TABLE EMPLOYEESAMPLE;
INSERT INTO EMPLOYEESAMPLE (id, name) VALUES
(1, 'John Smith'),
(2, 'Jane Doe'),
(3, 'Jack Johnson'),
(4, 'Jill Brown'),
(5, 'James Williams'),
(6, 'Emily Davis'),
(7, 'Michael Miller'),
(8, 'Sarah Wilson'),
(9, 'David Moore'),
(10, 'Laura Taylor'),
(11, 'Robert Anderson'),
(12, 'Linda Thomas'),
(13, 'William Jackson'),
(14, 'Elizabeth White'),
(15, 'James Harris'),
(16, 'Megan Clark'),
(17, 'John Lewis'),
(18, 'Patricia Hall'),
(19, 'Charles Allen'),
(20, 'Barbara Young'),
(21, 'Thomas Scott'),
(22, 'Nancy King'),
(23, 'Daniel Wright'),
(24, 'Helen Lopez'),
(25, 'Matthew Green'),
(26, 'Sandra Adams'),
(27, 'Joseph Baker'),
(28, 'Margaret Nelson'),
(29, 'Anthony Carter'),
(30, 'Lisa Mitchell'),
(31, 'Paul Roberts'),
(32, 'Betty Turner'),
(33, 'Andrew Phillips'),
(34, 'Rachel Campbell'),
(35, 'Steven Parker'),
(36, 'Dorothy Evans'),
(37, 'Brian Edwards'),
(38, 'Deborah Collins'),
(39, 'Kevin Stewart'),
(40, 'Shirley Morris'),
(41, 'George Cook'),
(42, 'Carol Rogers'),
(43, 'Timothy Murphy'),
(44, 'Kimberly Cooper'),
(45, 'Ronald Richardson'),
(46, 'Jessica Cox'),
(47, 'Frank Howard'),
(48, 'Emily Ward'),
(49, 'Gary Watson'),
(50, 'Rebecca Brooks'),
(51, 'Scott Sanders'),
(52, 'Laura Price'),
(53, 'Ethan Bennett'),
(54, 'Kathleen Wood'),
(55, 'Joshua Gray'),
(56, 'Cheryl James'),
(57, 'Alexander Bennett'),
(58, 'Angela Reed'),
(59, 'Justin Hughes'),
(60, 'Janet Turner'),
(61, 'Samuel Ross'),
(62, 'Pamela Lee'),
(63, 'Brandon Hughes'),
(64, 'Alice Collins'),
(65, 'Jeremy Bailey'),
(66, 'Nancy Richardson'),
(67, 'Evelyn Young'),
(68, 'Austin Morgan'),
(69, 'Martha Peterson'),
(70, 'Christian Hughes'),
(71, 'Beverly Cook'),
(72, 'Gary Morgan'),
(73, 'Tiffany Bell'),
(74, 'Craig Ward'),
(75, 'Nancy Garcia'),
(76, 'Dennis Clark'),
(77, 'Barbara Murphy'),
(78, 'Willie Evans'),
(79, 'Eleanor Ramirez'),
(80, 'Carlos Perry'),
(81, 'Joyce Rogers'),
(82, 'Martin Torres'),
(83, 'Diane Brooks'),
(84, 'Albert Walker'),
(85, 'Natalie Morris'),
(86, 'Eric Sanchez'),
(87, 'Katherine Adams'),
(88, 'Samuel Ramirez'),
(89, 'Helen Murphy'),
(90, 'Benjamin Price'),
(91, 'Rachel Sanchez'),
(92, 'Patrick Green'),
(93, 'Laura Murphy'),
(94, 'Jack Moore'),
(95, 'Cynthia Bailey'),
(96, 'Robert Clark'),
(97, 'Judy Harris'),
(98, 'Steven Harris'),
(99, 'Karen Davis'),
(100, 'Daniel Collins'),
(1000, 'Zoe Martin');





WITH similarity_scores AS (
    SELECT
        e1.name AS original_name,
        e2.name AS similar_name,
        JAROWINKLER_SIMILARITY(e1.name, e2.name) AS similarity_score
    FROM
        employee e1
        JOIN employee e2
        ON e1.name <> e2.name
),
ranked_scores AS (
    SELECT
        original_name,
        similar_name,
        similarity_score,
        ROW_NUMBER() OVER (PARTITION BY original_name ORDER BY similarity_score DESC) AS rank
    FROM
        similarity_scores
)
SELECT
    original_name,
    similar_name,
    similarity_score
FROM
    ranked_scores
WHERE
    rank <= 10
ORDER BY
    original_name,
    similarity_score DESC;
