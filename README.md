# oracle_simple_jsonresolve

oracle json 解析的临时处理方式
参考来自：http://www.oschina.net/code/snippet_1162040_48289

并处理解决了：value内如果用：符号出现的截取错误。

使用范例1：
select json.parsejson('{"rta":"0.19","status":"0","msg":"PING OK - Packet loss \u003d 0%, RTA \u003d 0.19 ms","packetloss":"0"}','rta') from dual;

使用范例2(具体参照)：
declare 
rta varchar2(10) := null;
status varchar2(10) :=null;
msg varchar2(100) :=null;
packetloss varchar2(10) :=null;
JSON_ALL JSON_TYPE;

begin 
JSON_ALL :=json.t_parsejson('{"rta":"0.19","status":"0","msg":"PING OK - Packet loss \u003d 0%, RTA \u003d 0.19 ms","packetloss":"0"}');

rta  := JSON_ALL(rta);
status  :=JSON_ALL(status);
msg :=JSON_ALL(msg);
packetloss :=JSON_ALL(packetloss);

dbms_output.put_line(rta||','||status||','||msg||','||packetloss);
end
