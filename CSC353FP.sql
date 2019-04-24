drop schema CSC353FP;
create schema  CSC353FP;
use  CSC353FP;

create table Team
(	id varchar(10),
	primary key (id)
);

create table Game
(	game_id int(15),
	team1 varchar(10),
	team2 varchar(10),
	game_date varchar(50),
	primary key (game_id),
	foreign key(team1) references Team (id),
	foreign key(team2) references Team (id)
);


create table Play
(	play_id int(15),
	game_id	int(15),
	game_quarter int(3),
	play_down int(3),
	play_distance int(3),
	field_position int(5),
	play_type varchar(15),
	yards_gained int(5),
	touchdown boolean,
	primary key (play_id, game_id),
	foreign key (game_id) references Game (game_id)
);

