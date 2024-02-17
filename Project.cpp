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

class ColumnNames_CSV
{
public:
    string time;
    string latitude;
    string longitude;
    string course;
    string kts;
    string mph;
    string altitudeFeet;
    string reportingFacility;

    ColumnNames_CSV(string t, string lat, string lon, string c, string k, string m, string alt, string rf)
    {
        time = t;
        latitude = lat;
        longitude = lon;
        course = c;
        kts = k;
        mph = m;
        altitudeFeet = alt;
        reportingFacility = rf;
    }

    void display() const
    {
        cout << "Time: " << time << endl;
        cout << "Latitude: " << latitude << endl;
        cout << "Longitude: " << longitude << endl;
        cout << "Course: " << course << endl;
        cout << "Kts: " << kts << endl;
        cout << "MPH:" << mph << endl;
        cout << "AltitudeFeet: " << altitudeFeet << endl;
        cout << "ReportingFacility: " << reportingFacility << endl;
        cout << endl;
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

bool Time_Compare(const ColumnNames_CSV& a, const ColumnNames_CSV& b)
{
    return a.time < b.time;
}

int main() {

    string FilePath = "E:\\LTX Programming Assignment - SDE Qt_NZ5_flightdatar.csv";

    /*string FilePath;
    cout << "Enter CSV file path: ";
    getline(cin, FilePath);*/
    

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
    int choice,choice2;
    cout << "\nEnter your choice \n1: Read file, \n2: Parse file, \n3: Sort Data/Printing json file/Write to database, \n4: exit\nYour Choice: ";
    cin >> choice;
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
        sort(columnNames.begin(), columnNames.end(), Time_Compare);
        for (auto data : columnNames) {
            data.display();
        }
    choice_point_again:
        cout << "\nEnter your choice:\n1: Printing into json file,\n2: Storing in database,\n3: Exit,\nYour Choice:";
        cin >> choice2;
        if (choice2 == 1)
        {
            json jsonArray;
            cout << "Printing into json file..." << endl;
            for (auto data : columnNames)
            {
                jsonArray.push_back(data.toJSON());
            }

            ofstream jsonFile("output.json");
            jsonFile << jsonArray.dump(4);
            jsonFile.close();
            cout << "Printed..." << endl;
            goto choice_point_again;


        }
        else if (choice2 == 2) {
            Driver* driver;
            Connection* con;
            Statement* stmt;
            PreparedStatement* pstmt;

            const string server = "localhost";
            const string username = "root";
            const string password = "12345";
            const string database = "abc";

            /*string server, username, password, database;

            cout << "Enter the MySQL server address (e.g., tcp://localhost:3306): ";
            getline(cin, server);
            cout << "Enter the MySQL username: ";
            getline(cin, username);
            cout << "Enter the MySQL password: ";
            getline(cin, password);
            cout << "Enter the MySQL database name: ";
            getline(cin, database);*/

            try
            {
                driver = get_driver_instance();
                con = driver->connect(server, username, password);
                con->setSchema(database);

                stmt = con->createStatement();
                stmt->execute("CREATE TABLE IF NOT EXISTS asdf(Time VARCHAR(20), Latitude VARCHAR(20), Longitude VARCHAR(20), Course VARCHAR(20), kts VARCHAR(20), mph VARCHAR(20), AltitudeFeet VARCHAR(20), ReportingFacility VARCHAR(100))");
                cout << "Finished creating table" << endl;
                delete stmt;

                pstmt = con->prepareStatement("INSERT INTO asdf(Time, Latitude, Longitude, Course, kts, mph, AltitudeFeet, ReportingFacility) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

                for (auto data : columnNames) {
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
            goto choice_point_again;
        }
        else
        {

			cout << endl;    
			cout << "Thank You for using application";
            break;
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