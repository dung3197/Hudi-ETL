class InputValidationError(ValueError):
   def __init__(self, messages:str):
      super().__init__(f"Input Validation Error. \n Details: \n {messages}")