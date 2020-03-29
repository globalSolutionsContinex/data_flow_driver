import uuid

import DataFlow.Descriptor.dictionary as dictionary
import DataFlow.Descriptor.utils as utils
import DataFlow.Transformator.constants as trans_constants
import DataFlow.constants as constants

utils.mapper = dictionary.dictionary

class CanonicalFormat:

    def __init__(self, descriptor):
        self.descriptor = descriptor
        self.descriptor[trans_constants.ID_THOR] = {trans_constants.COLUMNS_FILE: [trans_constants.ID_THOR]}
        self.ERROR = 'error'
        self.WARNING = 'warning'
        self.record_canonical_rules = {}
        self.record_canonical_dictionary = {}

    def get_formatted_records(self, records, errors_dict, filename):
        canonical_products = []
        for i, record in enumerate(records):
            record["error"] = None
            record["file_origin"] = filename
            if trans_constants.ID_THOR not in record:
                record[trans_constants.ID_THOR] = uuid.uuid4().hex
                errors_dict[record[trans_constants.ID_THOR]] = []
            self.record_canonical_rules = {}
            self.record_canonical_dictionary = {}
            # Generates the dictionary with all the required fields
            self.generate_dictionary(record)
            if self.record_canonical_dictionary:
                canonical_products.append(self.record_canonical_dictionary)
                record["state"] = constants.LOADER_CANONICAL_FORMATTER
                record["error"] = None
            errors_dict[record[trans_constants.ID_THOR]].append({'state': record["state"], 'error': record["error"],
                                                                 'warning': record["warning"]})
        return canonical_products

    def generate_dictionary(self, record):
        try:
            self.map_columns(record)
            if bool(self.record_canonical_dictionary):
                self.apply_rules(record)
        except Exception as ex:
            record["state"] = constants.ERROR_LOADER_CANONICAL_FORMATTER
            record["error"] = str(ex)
            self.record_canonical_dictionary = {}

    def map_columns(self, record):
        canonical_descriptor = self.descriptor
        record["warning"] = []
        # iterate over the columns descriptor to transform
        for index, attribute in enumerate(canonical_descriptor):
            try:
                parameters = canonical_descriptor[attribute]
                # list of columns that the canonical is compose, list size is >= 0
                columns_list = parameters[
                    trans_constants.COLUMNS_FILE] if trans_constants.COLUMNS_FILE in parameters else []
                # pattern to join the columns retailer to canonical
                columns_pattern = parameters[
                    trans_constants.COLUMNS_PATTERN] if trans_constants.COLUMNS_PATTERN in parameters else '{}'
                column_values = [record[k] if k in record else None for k in columns_list]
                # verify if exist a map between retailer and canonical dictionaries if no put de default value
                column_values = [parameters[trans_constants.DEFAULT]] if len(column_values) == 0 else column_values
                # extract the list of rules
                self.record_canonical_rules[attribute] = parameters[
                    trans_constants.RULES] if trans_constants.RULES in parameters else []
                # always column_values has to have at least 1 values in his list
                self.record_canonical_dictionary[attribute] = column_values[0] if columns_pattern == '{}' else str(columns_pattern).format(*column_values)
            except Exception as ex:
                record["state"] = constants.ERROR_LOADER_CANONICAL_FORMATTER
                record["error"] = attribute + " #### " + str(ex)
                self.record_canonical_dictionary = {}
                break

    def apply_rules(self, record):
        # apply rules to canonical dictionary
        for index, can_attribute in enumerate(self.record_canonical_rules):
            if not self.record_canonical_dictionary:
                break
            rules = self.record_canonical_rules[can_attribute]
            # eval all the rules for the current column
            for rule in rules:
                try:
                    value = self.eval_rule(rule)
                    if self.ERROR in value:
                        record["state"] = constants.ERROR_LOADER_CANONICAL_FORMATTER
                        record["error"] = can_attribute + " ## " + value[self.ERROR]
                        self.record_canonical_dictionary = {}
                        break
                    if self.WARNING in value:
                        record["warning"] = record["warning"] + value[self.WARNING]
                    self.record_canonical_dictionary[can_attribute] = value['result']
                except Exception as ex:
                    record["state"] = constants.ERROR_LOADER_CANONICAL_FORMATTER
                    record["error"] = can_attribute + " ### " + str(ex)
                    self.record_canonical_dictionary = {}
                    break

    def eval_rule(self, command):
        try:
            com = command[trans_constants.LAMB_FUNC]
            if type(com) == str:
                command[trans_constants.LAMB_FUNC] = eval(com)
            lamb_func = command[trans_constants.LAMB_FUNC]
            result, warning = lamb_func(self.record_canonical_dictionary)
            if warning is None:
                return {'result': result}
            return {self.WARNING: [warning], 'result': result}
        except Exception as ex:
            return {self.ERROR: str(ex)}
