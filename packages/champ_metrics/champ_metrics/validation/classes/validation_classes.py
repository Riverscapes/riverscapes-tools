

class ValidationResult(object):
    def __init__(self, data_class="", test_name="", status="NotTested", message=""):
        self.data_class = data_class
        self.test_name = test_name
        self.status = status
        self.message = message

    def load_from_dict(self, dictionary):
        self.data_class = dictionary["DataClass"]
        self.test_name = dictionary["TestName"]
        self.status = dictionary["Status"]
        self.message = dictionary["Message"]

    def pass_validation(self, message=""):
        self.status = "Pass"
        if message:
            self.message = message

    def warning(self, message=""):
        self.status = "Warning"
        if message:
            self.message = message

    def error(self, message=""):
        self.status = "Error"
        if message:
            self.message = message

    def get_dict(self):

        return {"DataClass": self.data_class,
                "TestName": self.test_name,
                "Status": self.status,
                "Message": self.message}


def get_result_status(list_results, testname):

    return next(r for r in list_results if r["TestName"] == testname)["Status"]
