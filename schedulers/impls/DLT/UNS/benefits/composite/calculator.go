package composite

import (
	"UNSAdapter/pb_gen/objects"
	"UNSAdapter/predictor/interfaces"
	"UNSAdapter/schedulers/impls/DLT/UNS/benefits/base"
	interfaces2 "UNSAdapter/schedulers/impls/DLT/UNS/benefits/interfaces"
	"UNSAdapter/schedulers/partition"
)

type Calculator struct {
	Calculator2Coefficient map[interfaces2.Calculator]float64
	*base.CalculatorCommon
}

func (c *Calculator) PrioritySort(pc *partition.Context, jobs map[string]*objects.Job, predictor interfaces.Predictor) map[string]int {
	panic("implement me")
}

func NewCalculator() *Calculator {
	common := &base.CalculatorCommon{}
	c := &Calculator{}
	common.Impl = c
	c.CalculatorCommon = common
	return c
}

func (c *Calculator) Calculate(stub interface{}) interfaces2.Benefit {
	s := stub.(*Stub)
	resultBenefit := interfaces2.Benefit(0.)
	for calculator, coefficient := range c.Calculator2Coefficient {
		benefit := calculator.(base.CalculatorTemplate).Calculate(s.Calculator2Stub[calculator])
		resultBenefit += interfaces2.Benefit(coefficient) * benefit
	}
	return resultBenefit
}

func (c *Calculator) NewStub() interface{} {
	return &Stub{Calculator2Stub: make(map[interfaces2.Calculator]interface{})}
}

func (c *Calculator) UpdateStub(pc *partition.Context, contexts map[string]*base.BenefitCalculationContext, stub interface{}) {
	s := stub.(*Stub)
	for calculator, calculatorStub := range s.Calculator2Stub {
		calculator.(base.CalculatorTemplate).UpdateStub(pc, contexts, calculatorStub)
	}
}

type Stub struct {
	Calculator2Stub map[interfaces2.Calculator]interface{}
}

//func (c *Calculator) ByPredictIncrementally(pc *partition.Context, allocationsPredictResult interfaces.PredictResult, prevStub interface{}) (benefit interfaces2.Benefit, stub interface{}) {
//	s := prevStub.(*Stub)
//	s = c.CloneStub(s).(*Stub)
//	resultBenefit := interfaces2.Benefit(0.)
//	for calculator, coefficient := range c.Calculator2Coefficient {
//		benefit, tempStub := calculator.ByPredictIncrementally(pc, allocationsPredictResult, s.Calculator2Stub[calculator])
//		s.Calculator2Stub[calculator] = tempStub
//		resultBenefit += interfaces2.Benefit(coefficient) * benefit
//	}
//	return resultBenefit, s
//}

func (c *Calculator) CloneStub(stub interface{}) interface{} {
	s := &Stub{Calculator2Stub: map[interfaces2.Calculator]interface{}{}}
	oStub := stub.(*Stub)
	for calculator, stub := range oStub.Calculator2Stub {
		s.Calculator2Stub[calculator] = calculator.CloneStub(stub)
	}
	return s
}

//
//func (c *Calculator) ByPredict(pc *partition.Context, allocationsPredictResult interfaces.PredictResult) (benefit interfaces2.Benefit, stub interface{}) {
//	s := &Stub{
//		Calculator2Stub: make(map[interfaces2.Calculator]interface{}),
//	}
//	resultBenefit := interfaces2.Benefit(0.)
//	for calculator, coefficient := range c.Calculator2Coefficient {
//		benefit, tempStub := calculator.ByPredict(pc, allocationsPredictResult)
//		s.Calculator2Stub[calculator] = tempStub
//		resultBenefit += interfaces2.Benefit(coefficient) * benefit
//	}
//	return resultBenefit, s
//}
