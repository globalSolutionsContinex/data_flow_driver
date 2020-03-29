import DataFlow.DataDestination.controller as controller


def set_up():
    s3 = {
        "bucketName": "dev-thor-jobs",
        "accessKey": "",
        "secretKey": ""
    }
    des_controller = controller.DataDestination("test", "staging", s3)
    return des_controller


def test_get_columns():
    print("test_get_columns")
    data = [{"name": "s", "description": "desc"}, {"name": "s", "description": "desc"}]
    des_controller = set_up()
    columns = des_controller.get_columns_format(data)
    assert (columns == "(name,description)" or columns == "(description,name)")


def test_convert_to_insert_format():
    print("test_convert_to_insert_format")
    data = [{"name": "s", "description": 0}]
    des_controller = set_up()
    columns = des_controller.convert_to_insert_format(data)
    assert (columns == "('s',0)" or columns == "(0,'s')")


def test_delete_metada():
    print("test_delete_metada")
    data = [{"name": "s", "id_thor": 0}]
    des_controller = set_up()
    des_controller.delete_metadata(data)
    assert 'id_thor' not in data


def test_get_on_duplicate_format():
    print("test_get_on_duplicate_format")
    data = [{"name": "s", "description": 0}]
    des_controller = set_up()
    format = des_controller.get_on_duplicate_format(data)
    assert (format == "name = values(name),description = values(description)" or
            format == "description = values(description),name = values(name)")


def run_all_test():
    test_convert_to_insert_format()
    test_delete_metada()
    test_get_columns()
    test_get_on_duplicate_format()
