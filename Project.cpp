#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <algorithm>
#include <nlohmann/json.hpp>
#include "mysql_connection.h"
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/prepared_statement.h>

using namespace std;
using json = nlohmann::json;
using namespace sql;

struct ColumnNames_CSV {
    string time;
    string latitude;
    string longitude;
    string course;
    string kts;
    string mph;
    string altitudeFeet;
    string reportingFacility;

    ColumnNames_CSV(string t, string lat, string lon, string c, string k, string m, string alt, string rf) :
        time(t), latitude(lat), longitude(lon), course(c), kts(k), mph(m), altitudeFeet(alt), reportingFacility(rf) {}

    void display() const {
        cout << "\nTime: " << time << endl;
        cout << "Latitude: " << latitude << endl;
        cout << "Longitude: " << longitude << endl;
        cout << "Course: " << course << endl;
        cout << "KTS: " << kts << endl;
        cout << "MPH: " << mph << endl;
        cout << "AltitudeFeet: " << altitudeFeet << endl;
        cout << "Reporting Facility : " << reportingFacility << endl;
    }


    json toJSON() const
    {
        json jsonObject;
        jsonObject["Time"] = time;
        jsonObject["Latitude"] = latitude;
        jsonObject["Longitude"] = longitude;
        jsonObject["Course"] = course;
        jsonObject["Kts"] = kts;
        jsonObject["MPH"] = mph;
        jsonObject["AltitudeFeet"] = altitudeFeet;
        jsonObject["ReportingFacility"] = reportingFacility;
        return jsonObject;
    }
};

bool Sort_By_Time(const ColumnNames_CSV& a, const ColumnNames_CSV& b) {
    string t1 = a.time;
    string t2 = b.time;

    if (t1.substr(t1.length() - 2) == "PM" && t1.substr(0, 2) != "12") {
        int hour = stoi(t1.substr(0, 2));
        hour += 12;
        t1.replace(0, 2, to_string(hour));
    }
    else if (t1.substr(t1.length() - 2) == "AM" && t1.substr(0, 2) == "12") {
        t1.replace(0, 2, "00");
    }

    if (t2.substr(t2.length() - 2) == "PM" && t2.substr(0, 2) != "12") {
        int hour = stoi(t2.substr(0, 2));
        hour += 12;
        t2.replace(0, 2, to_string(hour));
    }
    else if (t2.substr(t2.length() - 2) == "AM" && t2.substr(0, 2) == "12") {
        t2.replace(0, 2, "00");
    }

    return t1 < t2;
}

int main() {

    //string FilePath = "C:\\Users\\harsh\\OneDrive\\Desktop\\Data.csv";

    string FilePath;
    cout << "Enter CSV file path: ";
    getline(cin, FilePath);
    

    ifstream ReadFile;
    ReadFile.open(FilePath);

    if (!ReadFile.is_open()) {
        cout << "Error........." << endl;
        return 1;
    }

    cout << "File Opened Successfully. Now what you want to do, Select appropriate  option:";
    vector<ColumnNames_CSV> columnNames;

    string row = "";
    getline(ReadFile, row);
    json jsonArray;
    while (getline(ReadFile, row)) {
        string time;
        string latitude;
        string longitude;
        string course;
        string kts;
        string mph;
        string altitudeFeet;
        string reportingFacility;
        stringstream inputString(row);
        getline(inputString, time, ',');
        getline(inputString, latitude, ',');
        getline(inputString, longitude, ',');
        getline(inputString, course, ',');
        getline(inputString, kts, ',');
        getline(inputString, mph, ',');
        getline(inputString, altitudeFeet, ',');
        getline(inputString, reportingFacility, ',');

        ColumnNames_CSV data(time, latitude, longitude, course, kts, mph, altitudeFeet, reportingFacility);

        columnNames.push_back(data);
    }

choice_point:
    int choice;
    cout << "\nEnter your choice \n1: Read file, \n2: Parse file, \n3: Sort Data/Printing json file/Write to database, \n4: exit\nYour Choice: ";
    cin >> choice;
    int choice2;
    switch (choice) {
    case 1:
        cout << "\nFile Opened Successfully...Showing File: " << endl;
        ReadFile.close();
        ReadFile.open(FilePath);
        while (getline(ReadFile, row)) {
            cout << row << endl;
        }
        goto choice_point;
        break;

    case 2:
        cout << "\nParsing CSV File data into array of structs/objects..." << endl;
        for (auto data : columnNames) {
            data.display();
        }
        goto choice_point;
        break;

    case 3:
        cout << "\nSorting data..." << endl;
        sort(columnNames.begin(), columnNames.end(), Sort_By_Time);
        for (auto data : columnNames) {
            data.display();
        }

        cout << "\nEnter your choice:\n1: Printing into json file,\n2: Storing in database,\n3: Exit,\nYour Choice:";
        cin >> choice2;

        if (choice2 == 1)
        {
            cout << "Printing into json file..." << endl;
            for (const auto& data2 : columnNames)
            {
                jsonArray.push_back(data2.toJSON());
            }

            ofstream jsonFile("output.json");
            jsonFile << jsonArray.dump(4);
            jsonFile.close();
            cout << "Printed..." << endl;

        }
        else if (choice2 == 2) {
            Driver* driver;
            Connection* con;
            Statement* stmt;
            PreparedStatement* pstmt;

            /*const string server = "tcp://localhost:3306";
            const string username = "root";
            const string password = "12345";
            const string database = "abc";*/

            string server, username, password, database;

            cout << "Enter the MySQL server address (e.g., tcp://localhost:3306): ";
            getline(cin, server);
            cout << "Enter the MySQL username: ";
            getline(cin, username);
            cout << "Enter the MySQL password: ";
            getline(cin, password);
            cout << "Enter the MySQL database name: ";
            getline(cin, database);

            try
            {
                driver = get_driver_instance();
                con = driver->connect(server, username, password);
                con->setSchema(database);

                stmt = con->createStatement();
                stmt->execute("CREATE TABLE IF NOT EXISTS Project_Data(Time VARCHAR(20), Latitude VARCHAR(20), Longitude VARCHAR(20), Course VARCHAR(20), kts VARCHAR(20), mph VARCHAR(20), AltitudeFeet VARCHAR(20), ReportingFacility VARCHAR(100))");
                cout << "Finished creating table" << endl;
                delete stmt;

                pstmt = con->prepareStatement("INSERT INTO FlightData(Time, Latitude, Longitude, Course, kts, mph, AltitudeFeet, ReportingFacility) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

                for (const auto& data : columnNames) {
                    pstmt->setString(1, data.time);
                    pstmt->setString(2, data.latitude);
                    pstmt->setString(3, data.longitude);
                    pstmt->setString(4, data.course);
                    pstmt->setString(5, data.kts);
                    pstmt->setString(6, data.mph);
                    pstmt->setString(7, data.altitudeFeet);
                    pstmt->setString(8, data.reportingFacility);

                    pstmt->executeUpdate();
                }
                cout << "Data stored..." << endl;
            }
            catch (SQLException)
            {
                cout << "Could not connect to server" << endl;
                system("pause");
                exit(1);
            }
            delete pstmt;
        }
        else
        {
            cout << endl;
        }

        goto choice_point;
        break;

    case 4:
        cout << "Thank You for using application";
        goto exit_point;
        break;

    default:
        cout << "Invalid choice!" << endl;
        break;
    }

exit_point:
    ReadFile.close();
    return 0;
}