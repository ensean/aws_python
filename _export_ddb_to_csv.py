import boto3
import csv


TABLE_NAME = "example_table"


def save_list_to_csv(input_list, file_path):
    out = open(file_path, 'a', encoding="utf-8",
               newline='')
    csv_write = csv.writer(out, dialect='excel')
    csv_write.writerow(input_list)


def export_to_csv():
    dynamodb_resource = boto3.resource('dynamodb')
    table = dynamodb_resource.Table(TABLE_NAME)
    response = table.scan()
    items = response['Items']
    key_list = []
    key_dict = {}
    for item in items:
        for key in item:
            key_dict[key] = 1
    for key in key_dict:
        key_list.append(key)
    save_list_to_csv(key_list, "result.csv")
    for item in items:
        csv_item = []
        for key in key_dict:
            if key in item:
                csv_item.append(item[key])
            else:
                csv_item.append("")
        save_list_to_csv(csv_item, "result.csv")


if __name__ == '__main__':
    export_to_csv()