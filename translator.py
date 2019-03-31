import re
import unittest

class Translator(object):

    def __init__(self, dictionary = False):
        self._dictionary = dictionary

    def value(self, question_name, value, year = False):

        if not isinstance(question_name, str):
            raise TypeError("Question name {} is not valid".format(question_name))

        if not isinstance(value, (int, float)):
            raise TypeError("Input value {} is not valid".format(value))

        if not isinstance(year, (int, bool)):
            raise TypeError("Input year {} is not valid".format(year))

        if year and year < 1979 or year > 2015:
            raise ValueError("Input year {} is not between 1979 and 2015".format(year))

        # First, check to see if the given "question_name" exists in our translation dictionary.

        if question_name in self._dictionary:
            question_dict = self._dictionary[question_name]
        else:

            # If the key requested doesn't exist, check to see if it matches a regex in the dictionary.
            # This allows us to reuse the same translation dictionary for very similar questions,
            # e.g. "HGCREV10" and "HGCREV12", without having to repeat the entire dictionary.

            dict_keys = self._dictionary.keys()
            question_found = False

            for key in dict_keys:
                if re.match(key, question_name):
                    question_name = key
                    question_dict = self._dictionary[question_name]
                    question_found = True
                    break

            if not question_found:
                raise KeyError("No dictionary exists for the question name {}".format(question_name))

        # In some instances, the dictionary object will equal False -- meaning no translation is needed;
        # the object can be returned as is.

        if question_dict is False:
            return(value)
        else:

            if "minyear" in question_dict:
                if year and year < question_dict["minyear"]:
                    raise ValueError("Input year {} is not valid. Question name {} applies only to years after {}".format(year, question_name, question_dict["minyear"]))

            if "maxyear" in question_dict:
                if year and year > question_dict["minyear"]:
                    raise ValueError("Input year {} is not valid. Question name {} applies only to years after {}".format(year, question_name, question_dict["minyear"]))

            # If a translation dictionary exists, check to see if it has a "type" key. If so, that's a
            # sign that this is a special variable, such as an income number that needs inflation
            # adjustment.

            if "type" in question_dict:
                if question_dict["type"] == "income":
                    if not year:
                        raise TypeError("No year was given. Dollar values must be accompanied by a year for inflation adjustment")

                    if str(year) in question_dict:
                        if value == question_dict[str(year)]:
                            return("topcode")
                        elif value > question_dict[str(year)]:
                            raise ValueError("Input value {} is greater than the topcode value for {}".format(value, year))

                    if year < 2015:
                        adjust_year = year
                        while adjust_year < 2015:
                            inflation_adjustment = self._dictionary[str(adjust_year)]
                            value = value + value * inflation_adjustment
                            adjust_year += 1
                        return(value)
                    else:
                        return(value)

            else:

                # If none of those conditions apply, we're dealing with a simple translation
                # dictionary, so we can simply look up the value's translation (or raise
                # an error if no translation exists).

                if str(value) in question_dict:
                    return(question_dict[str(value)])
                else:

                    # Check to see if a regex in the dictionary matches the value.
                    dict_keys = question_dict.keys()

                    for key in dict_keys:
                        if re.match(key, str(value)):

                            # If the regex gives an explicit translation, make it; otherwise,
                            # return the initial value.

                            if question_dict[key] is False:

                                # This distinguishes between "False," which means to return the
                                # value unchanged, and 0, which means to return the value 0.
                                return(value)

                            else:
                                return(question_dict[key])

                    if "other" in dict_keys:

                        # This is a special value providing the handling for any value that doesn't
                        # otherwise match an entry.

                        if question_dict["other"] is False:
                            return(value)
                        else:
                            return(question_dict[key])

            raise KeyError("No dictionary entry exists for the value {}".format(value))


class TestTranslatorFunctions(unittest.TestCase):

    def setUp(self):
        import json
        with open("dict.json") as json_file:
            self.translator = Translator(json.load(json_file))

    def test_invalid_key(self):
        # "BADKEY" isn't a valid question code.
        self.assertRaises(KeyError, self.translator.value, "BADKEY", 1)
        self.assertRaises(TypeError, self.translator.value, 7, 1)
        self.assertRaises(TypeError, self.translator.value, {"3": 1}, 2)

    def test_topcoding(self):
        # $42,458 is the topcode value for 2002.
        self.assertEqual(self.translator.value("YINC-1700", 42458, 2002), "topcode")
        self.assertRaises(ValueError, self.translator.value, "YINC-1700", 43000, 2002)

        # This value will inflation-adjust *above* the topcoded limit for 2008,
        # but as it's an inflation-adjusted figure (rather than an actual input for the
        # 2008 year), that shouldn't raise an error.
        self.assertGreater(self.translator.value("YINC-1700", 112000, 2007), 120000)

    def test_inflation_adjustment(self):
        # This is a very rough test; it just ensures that inflation adjustment
        # generates a figure in roughly the right range, as we may tweak our
        # inflation adjustment values over time.
        self.assertGreater(self.translator.value("Q13-5", 1000, 1979), 2000)
        self.assertLess(self.translator.value("Q13-5", 1000, 1979), 5000)

    def test_race_coding(self):
        self.assertEqual(self.translator.value("KEY!RACE_ETHNICITY", 4), 3)
        self.assertEqual(self.translator.value("KEY!RACE_ETHNICITY", 3), 3)
        self.assertEqual(self.translator.value("KEY!RACE_ETHNICITY", -5), -5)

    def test_key_regex(self):
        # The input 93 is an error code that should be translated into a 0
        # via a regular expression in the dictionary.
        self.assertEqual(self.translator.value("YSCH-3112", 93), 0)

    def test_value_regex(self):
        # HGCREV10 is handled via the "HGCREV*" regular expression in the dictionary.
        self.assertEqual(self.translator.value("HGCREV10", 12), 12)
        self.assertEqual(self.translator.value("HGCREV10", 93), 0)

    def test_invalid_value(self):
        self.assertRaises(TypeError, self.translator.value, "SAMPLE_SEX", "BADKEY")

    def test_invalid_year(self):
        self.assertRaises(ValueError, self.translator.value, "YINC-1700", 1000, 2016)
        self.assertRaises(ValueError, self.translator.value, "Q13-5", 1000, 1978)
        self.assertRaises(TypeError, self.translator.value, "Q13-5", 1000, "BADYEAR")
        self.assertRaises(TypeError, self.translator.value, "Q13-5", 1000)
        self.assertRaises(ValueError, self.translator.value, "FFER-13", 1, 1992)
        self.assertRaises(ValueError, self.translator.value, "Q11-5C", 1, 1990)

    def test_out_of_range_year(self):
        self.assertRaises(ValueError, self.translator.value, "Q13-5_TRUNC_REVISED", 1000, 1980)
        self.assertRaises(ValueError, self.translator.value, "Q13-5_TRUNC_REVISED", 1000, 2001)

if __name__ == '__main__':
    unittest.main()
