# Stedi-Human-Balance-Analytics
This is an analytics project where there are 3 datasets provided for customer, accelerometer and steptrainer. We have to filter out data and make AWS glue jobs which will be useful for analysis
CustomerLanding -> CustomerTrusted
AccelerometerLanding + CustomerTrusted -> AcceleromterTrusted
CustomerTrusted + AccelerometerTrusted -> CustomerCurated
SteptrainerLanding + customerCurated -> SteptrainerTrusted
SteptrainerTrusted + AccelerometerTrusted -> MachineLearningCurated
