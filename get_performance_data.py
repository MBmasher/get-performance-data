#!/usr/bin/env python3

from bs4 import BeautifulSoup
import argparse
import requests
from tqdm import tqdm
from datetime import date
import signal
import tarfile
import csv
import os


def get_latest(mode, type):
    page_link = "https://data.ppy.sh"
    page_response = requests.get(page_link, timeout=5)
    page_content = BeautifulSoup(page_response.content, "html.parser")
    page_links = page_content.find_all("a")

    filtered_list = []

    for page in page_links:
        current_page = "".join(page.findAll(text=True)).split("\n")[0]
        filename = current_page.split(".")[0]
        file_underscore_split = filename.split("_")

        if len(file_underscore_split) < 6:
            continue

        if file_underscore_split[-2] == mode and file_underscore_split[-1] == type:
            year = int(file_underscore_split[0])
            month = int(file_underscore_split[1])
            day = int(file_underscore_split[2])

            file_date = date(year, month, day)

            filtered_list.append([file_date, current_page])

    latest_file = max(filtered_list, key=lambda i: i[0])[1]

    return latest_file


def download_file(filename, directory):
    page_link = "https://data.ppy.sh/{}".format(filename)
    page_response = requests.get(page_link)

    print("Creating file to write to...")
    zip_file = open(directory + filename, "wb+")

    print("Starting download...")
    dl = 0
    for data in tqdm(page_response.iter_content(chunk_size=4096)):
        dl += len(data)
        zip_file.write(data)

    zip_file.close()


def unzip_file(filename, directory):
    print("Unzipping file...")
    tar = tarfile.open(directory + filename, "r:bz2")

    for member_info in tar.getmembers():
        print("- extracting: " + member_info.name)
        tar.extract(member_info)

    tar.close()


def get_first_char(input_string):
    for character in input_string:
        if character != " " and character != "\t":
            return character

    return None


def sql_to_csv(directory):
    for sql_file in os.listdir(directory):
        if not os.path.isfile(os.path.join(directory, sql_file)):
            continue
        if sql_file.split(".")[-1] != "sql":
            continue

        print("- converting: " + sql_file)

        sql_filename = ".".join(sql_file.split(".")[:-1])

        sql_file_open = open(directory + sql_file, "r", encoding="utf8", errors='ignore')
        csv_file_open = open(directory + sql_filename + ".csv", "w+")

        sql_file_lines = [i.split("\n")[0] for i in sql_file_open.readlines()]

        headers = []
        getting_headers = False

        for line in tqdm(sql_file_lines):
            if line.startswith("CREATE TABLE"):
                getting_headers = True
                continue

            if getting_headers:
                if get_first_char(line) != "`":
                    getting_headers = False
                    csv_file_open.write(",".join(headers) + "\n")
                    continue

                headers.append(line.split("`")[1])

            if line.startswith("INSERT INTO"):
                values = line.partition("` VALUES ")[2]

                latest_row = []
                reader = csv.reader(
                    [values],
                    delimiter=",",
                    doublequote=False,
                    escapechar="\\",
                    quotechar="'",
                    strict=True,
                )
                writer = csv.writer(csv_file_open, quoting=csv.QUOTE_MINIMAL)

                for reader_row in reader:
                    for column in reader_row:
                        if len(column) == 0 or column == "NULL":
                            latest_row.append(chr(0))
                            continue

                        if column[0] == "(":
                            new_row = False

                            if len(latest_row) > 0:
                                if latest_row[-1][-1] == ")":
                                    latest_row[-1] = latest_row[-1][:-1]
                                    new_row = True

                            if new_row:
                                writer.writerow(latest_row)
                                latest_row = []

                            if len(latest_row) == 0:
                                column = column[1:]

                        latest_row.append(column)

                    if latest_row[-1][-2:] == ");":
                        latest_row[-1] = latest_row[-1][:-2]
                        writer.writerow(latest_row)

        sql_file_open.close()
        os.remove(directory + sql_file)

        csv_file_open.close()


def main():
    signal.signal(signal.SIGTERM, exit)
    signal.signal(signal.SIGINT, exit)
    signal.signal(signal.SIGTSTP, exit)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        nargs="?",
        default="osu",
        help="Modes: osu, taiko, fruits, mania (Defaults to osu)",
    )
    parser.add_argument(
        "--type", nargs="?", default="top", help="Types: top, random (Defaults to top)"
    )
    parser.add_argument(
        "--output-directory", nargs="?", default="./", help="(Defaults to ./)"
    )
    args = parser.parse_args()

    directory_final = args.output_directory
    if directory_final[-1] != "/":
        directory_final += "/"

    print("Getting latest file...")
    latest_file = get_latest(args.mode, args.type)
    print("Connecting to data.ppy.sh...")
    download_file(latest_file, directory_final)
    unzip_file(latest_file, directory_final)
    print("Converting all sql files to csv...")
    sql_to_csv(directory_final + latest_file.split(".")[0] + "/")


if __name__ == "__main__":
    main()
