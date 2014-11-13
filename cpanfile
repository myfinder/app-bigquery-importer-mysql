requires 'perl', '5.008001';
requires 'Class::Accessor::Lite';
requires 'Config::CmdRC';
requires 'DBI';
requires 'DBD::mysql';

on 'test' => sub {
    requires 'Test::More', '0.98';
};

