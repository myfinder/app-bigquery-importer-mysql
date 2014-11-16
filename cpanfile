requires 'perl', '5.008001';
requires 'Time::Piece';
requires 'Config::CmdRC';
requires 'DBI';
requires 'DBD::mysql';

on 'test' => sub {
    requires 'Test::More', '0.98';
};

