# 海通证券

## 基本信息

1. PrdCode: 307
2. AcctIDByBroker: 1882842000(123321)

## 关于计息期
1. 目前业务模式（当日锁定次日交易的股票）下，为合约期起始日，为计息期起始日。

2. 锁券合约若在最后交易日前一日清算时未被占用，则自动被系统收回，收回后无法开仓。

3. 计息期“计头不计尾”，[发生日, 最后交易日)。

4. 最后交易日为“理论到期日”的前一日。

5. “闲置费”与“融券息费”现在”两费合一”，计息期从锁券日当日起算（包含当日）。

6. “锁券”可发生于9:00~清算,锁券申请可且仅可于9:00~18:00提交。

7. 融券卖出可卖数量遵循“即锁即卖”原则。

8. 库存券池名单于每日9:00发布，实时更新。

## 费率
### ”即开即平再交易“模式与”直接开仓“模式成本差异分析

### （20201210 update）

1. 融券卖出与担保品卖出所有费率相同
2. 成本差异在于多周转一次 与 ”即开即平“产生的损益
3. 根据《鸣石满天星7号——证券备忘录（所有经纪商）20200214《定稿》(1)》附件二：证券交易参数表
交易费用（A股交易-上交所） = 0.013% * 2 （佣金:双边）+  0.00487% * 2 (证券交易经手费:双边) + 0.002% *2 （证券交易证管费：双边）+ 0.1%(证券交易印花税: 卖出单边) + 0.002%（证券交易过户费：仅限上交所有，单边收费） = 0.1417%
交易费用（A股交易-深交所） = 0.013% * 2 （佣金:双边）+  0.00487% * 2 (证券交易经手费:双边) + 0.002% *2 （证券交易监管费：双边）+ 0.1%(证券交易印花税: 卖出单边)  = 0.1397%
4. 佣金(后改为万分之一，免五)  = 海通证券收益 + 证管费（万分之0.487） + 监管费（万分之0.2）
5. FeeFromTradingActivity = 佣金 + 过户费（沪市特有）+ 印花税

## PNL分析（20201109 update）
### 名词约定
1. 成交价损益: 由且仅由成交价及成交数量计算的出的损益。
2. 交易费用(fees from trading activity): 由且仅由交易行为驱动发生的各项费用。
	1. post trddata 报告数据为交割单读取数据。
	2. 算法参考产品证券备忘录文件。
3. 券息费用: 由且仅由融券行为驱动发生的费用。
4. 策略交易费前损益:  归属于单一策略的交易费用、券息费用计提后、产品费用（如业绩报酬、管理费）计提前的净损益。

### 算法
1. 交易费用: 
   1. FeeFromTradingActivity = 佣金 + 过户费（沪市特有，深市为0）+ 印花税
2. 券息：
  1. 公共券池: 当日发生额: 利息备份
  2. 私用券源: 当日发生额: 占用额度费备份

## 策略实现的误差
### 1. 交易系统
1. 科创板交易最小委托数量为200股。
2. 深交所零股卖出时需要一次性全部卖出，导致产生零股，产生隔夜敞口。
3. 由于限时撤单原因，导致盘口价差大的股票的成交价与信号发出的价格偏差较大。

## 间接法计算现金流
1. 现金变动 = PNL+ 负债变动 - 非现金资产变动

   TODO

   1. 利息归本问题。三个月结息，季月20日支付。
      1. 本基金资金余额，按季度支付0.35%的年利率，每季度末月20日结息

