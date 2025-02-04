1. Weights are used to calculate average age of each household.
	- adult older than 20 (including), weight 1
	- person age between 10 (including) and 19 (including), weight 0.5
	- person younger than 10, weight 0.3
	- excel function is =(SUMPRODUCT((AI16:BC16>19)*AI16:BC16, (AI16:BC16>19)*1) + SUMPRODUCT((AI16:BC16>=10)*(AI16:BC16<=19)*AI16:BC16, (AI16:BC16>=10)*(AI16:BC16<=19)*0.5) + SUMPRODUCT((AI16:BC16<10)*(AI16:BC16>0)*AI16:BC16, (AI16:BC16<10)*(AI16:BC16>0)*0.3))/ (SUM((AI16:BC16>19)*1) + SUM((AI16:BC16>=10)*(AI16:BC16<=19)*0.5) + SUM((AI16:BC16<10)*(AI16:BC16>0)*0.3))