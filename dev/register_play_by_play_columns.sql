-- Register column ordering for nba.play_by_play table
-- 24 columns from PlayByPlayV3 API (after clean_names normalization)
-- origin_name is NULL because clean_names() handles the camelCase → snake_case conversion
-- column_name defines the order columns should appear in the table

INSERT INTO util.table_column_order 
(schema, table_name, column_name, origin_name, column_order)
VALUES
-- Core identifiers
('nba', 'play_by_play', 'game_id', NULL, 1),
('nba', 'play_by_play', 'action_number', NULL, 2),
('nba', 'play_by_play', 'action_id', NULL, 3),

-- Time/period info
('nba', 'play_by_play', 'period', NULL, 4),
('nba', 'play_by_play', 'clock', NULL, 5),

-- Team info
('nba', 'play_by_play', 'team_id', NULL, 6),
('nba', 'play_by_play', 'team_tricode', NULL, 7),

-- Player info
('nba', 'play_by_play', 'person_id', NULL, 8),
('nba', 'play_by_play', 'player_name', NULL, 9),
('nba', 'play_by_play', 'player_name_i', NULL, 10),

-- Shot/event details
('nba', 'play_by_play', 'action_type', NULL, 11),
('nba', 'play_by_play', 'sub_type', NULL, 12),
('nba', 'play_by_play', 'shot_distance', NULL, 13),
('nba', 'play_by_play', 'shot_result', NULL, 14),
('nba', 'play_by_play', 'is_field_goal', NULL, 15),
('nba', 'play_by_play', 'shot_value', NULL, 16),

-- Coordinates (renamed from x_legacy/y_legacy for clarity)
('nba', 'play_by_play', 'court_x', 'x_legacy', 17),
('nba', 'play_by_play', 'court_y', 'y_legacy', 18),
('nba', 'play_by_play', 'location', NULL, 19),

-- Score info
('nba', 'play_by_play', 'score_home', NULL, 20),
('nba', 'play_by_play', 'score_away', NULL, 21),
('nba', 'play_by_play', 'points_total', NULL, 22),

-- Description
('nba', 'play_by_play', 'description', NULL, 23),

-- Video
('nba', 'play_by_play', 'video_available', NULL, 24);
