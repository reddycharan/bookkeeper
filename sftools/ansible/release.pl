#!/usr/bin/perl

use strict;
use warnings;
use File::Copy::Recursive qw(fcopy rcopy dircopy fmove rmove dirmove);
use File::Path qw(make_path);

{
    my $pkgPath = $ARGV[0];

    # Update this path to your PerForce folder
    my $p4Path = "/home/zshi/Perforce/zshi_zshi-wsl1_5834";


    my $bkPath = $p4Path . "/apps/sfstorage/bookkeeper/main/";
    dircopy($bkPath, $pkgPath . "/bookkeeper/" ) or die $!;

    my $sfmsPath = $p4Path . "/tools/sayonara/prod/";
    dircopy($sfmsPath, $pkgPath . "/sayonara/" ) or die $!;

    my $dir = "/tools/ant/apache-ant-1.7.1/";
    dircopy($p4Path . $dir, $pkgPath . $dir) or die $!;

    $dir = "/tools/Linux/jdk/jdk1.8.0_60_x64/";
    dircopy($p4Path . $dir, $pkgPath . $dir) or die $!;

    system("tar -cjf $pkgPath.tar.bz2 $pkgPath") or die $!;

}
