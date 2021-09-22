from catenae import Link


class LoopFilter(Link):
    def setup(self):
        self.send("seed message")

    def transform(self, message):
        self.logger.log(message)
        return message


if __name__ == "__main__":
    LoopFilter(input_stream='stream0', default_output_stream='stream0').start()
