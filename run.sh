#bin/usr/env bash
if [ $1 -eq 0 ];then
        java -cp p1*/target/dfs*.jar edu.usfca.cs.dfs.Controller
elif [ $1 -eq 1 ];then
        java -cp p1*/target/dfs*.jar edu.usfca.cs.dfs.StorageNode
elif [ $1 -eq 2 ];then
        java -cp p1*/target/dfs*.jar edu.usfca.cs.dfs.Client
fi
