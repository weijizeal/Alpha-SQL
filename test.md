问题类型	数量	问题ID
✅ 完全正确	20	1,2,4,6,22,23,24,28,30,32,35,37,40,47,48,54,62,68,69,70
窗口函数LAG错误	5	18,25,26,33,42
日期解析错误	2	8,9
列选择错误(多了列)	7	5,12,13,19,38,51,52
列选择错误(少了列)	3	17,56,57
INTERSECT vs UNION混淆	2	65,66
LIKE vs = 错误	4	3,49,50,58
股票代码前缀错误	1	27
日期范围错误	2	10,11
概念代码错误	2	29,53
其他逻辑错误	11	7,27,31,34,36,39,41,45,46,55,60,67


问题	GT SQL	模型预测	原因
Q5	data_code	data_code, secuname	模型多选了 secuname
Q19	secucode	secucode, secuname	模型多选了 secuname
Q38	secucode	secucode, zdf, qrr	模型多选了 zdf, qrr
Q51	secucode	secucode, secuname	模型多选了 secuname
Q52	secucode	secucode, secuname	模型多选了 secuname

问题	GT SQL	模型预测	原因
Q56	secucode	parent_net_profit_yoy	完全选错列！模型只返回了增长率数值，没有返回股票代码
Q57	secucode	secucode	实际上一样？

分析"其他逻辑错误"：

问题	问题描述	错误类型
Q7	24年至25年6月上市的公司	用 date 而非 issue_list_date
Q27	液冷技术的股票	evidence 概念代码错误（已修复）
Q31	底位刚启动的半导体股票	逻辑复杂，需进一步分析
Q34	股票走强	应该是正确的
Q36	放量上涨的股票	应该是正确的
Q39	放量亿上涨的股票	"亿"指成交额 vs "放量"量比混淆
Q41	成长期和低股指股票	逻辑复杂，需进一步分析
Q45	山东与供热发电	GT用INTERSECT，模型用AND
Q46	券商板块包括那些行业	问行业返回股票
Q55	今天有多少热点股票	问数量但GT返回列表
Q60	今天的热门股票	与Q5重复
Q67	固态变压器	概念代码错误（已修复）