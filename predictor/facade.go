package predictor

import (
	"UNSAdapter/pb_gen"
	"UNSAdapter/pb_gen/configs"
	"UNSAdapter/predictor/dlt_predictor"
	"UNSAdapter/predictor/interfaces"
)

func BuildPredictor(configuration *configs.PredictorConfiguration) interfaces.Predictor {
	predictorType := configuration.GetPredictorType()
	switch predictorType {
	case configs.PredictorType_predictorTypeDLTRandom:
		return dlt_predictor.NewRandomPredictor(pb_gen.GetRandomPredictorConfiguration(configuration))
	case configs.PredictorType_predictorTypeDLTDataOriented:
		return dlt_predictor.NewDataOrientedPredictor(pb_gen.GetDataOrientedPredictorConfiguration(configuration))
	default:
		panic("Unsupported predictor type.")
	}
}
