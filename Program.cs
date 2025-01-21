using System.Data;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.SqlClient;
using Microsoft.VisualBasic.FileIO;

namespace TaxiTripETL;

public class Program
{
    private static readonly string ConnectionString = "Server=tcp:mainserv.database.windows.net,1433;Initial Catalog=TaxiTrips;Persist Security Info=False;User ID=yurii;Password={};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
    private static readonly TimeZoneInfo EstTimeZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");

    public static async Task Main(string[] args)
    {
        var inputFile = "C:\\Users\\Yura\\RiderProjects\\ConsoleApp1\\ConsoleApp1\\sample-cab-data.csv";
        var duplicatesFile = "duplicates.csv";
        var errorFile = "error_records.csv";

        try
        {
            var processor = new TaxiDataProcessor(ConnectionString, EstTimeZone);
            await processor.ProcessDataAsync(inputFile, duplicatesFile, errorFile);
            Console.WriteLine("Data processing completed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error processing file: {ex.Message}");
            Environment.Exit(1);
        }
    }
}
public class TaxiDataProcessor
{
    private readonly string _connectionString;
    private readonly TimeZoneInfo _timeZone;
    private readonly HashSet<TaxiTrip> _records;
    private readonly List<TaxiTrip> _duplicates;
    private readonly List<string> _errors;

    public TaxiDataProcessor(string connectionString, TimeZoneInfo timeZone)
    {
        _connectionString = connectionString;
        _timeZone = timeZone;
        _records = new HashSet<TaxiTrip>(new TaxiTripComparer());
        _duplicates = new List<TaxiTrip>();
        _errors = new List<string>();
    }

    public async Task ProcessDataAsync(string inputFile, string duplicatesFile, string errorFile)
    {
        Console.WriteLine("Starting to read CSV file...");
        
        using (var parser = new TextFieldParser(inputFile))
        {
            parser.TextFieldType = FieldType.Delimited;
            parser.SetDelimiters(",");
            parser.HasFieldsEnclosedInQuotes = true;

            // Skip the header
            parser.ReadLine();
            
            int lineNumber = 1;
            while (!parser.EndOfData)
            {
                try
                {
                    string[] fields = parser.ReadFields() ?? Array.Empty<string>();
                    if (fields.Length < 14)
                    {
                        _errors.Add($"Line {lineNumber}: Not enough fields");
                        continue;
                    }

                    if (TryParseRecord(fields, lineNumber, out var record))
                    {
                        if (!_records.Add(record))
                        {
                            _duplicates.Add(record);
                        }
                    }

                    if (lineNumber % 1000 == 0)
                    {
                        Console.WriteLine($"Processed {lineNumber} lines...");
                    }
                }
                catch (Exception ex)
                {
                    _errors.Add($"Line {lineNumber}: {ex.Message}");
                }
                lineNumber++;
            }
        }

        Console.WriteLine($"Found {_records.Count} valid records");
        Console.WriteLine($"Found {_duplicates.Count} duplicate records");
        Console.WriteLine($"Found {_errors.Count} errors");

        if (_errors.Any())
        {
            await File.WriteAllLinesAsync(errorFile, _errors);
            Console.WriteLine($"Errors written to {errorFile}");
        }

        if (_duplicates.Any())
        {
            await WriteRecordsToCSV(_duplicates, duplicatesFile);
            Console.WriteLine($"Duplicates written to {duplicatesFile}");
        }

        if (_records.Any())
        {
            await BulkInsertRecordsAsync(_records);
            Console.WriteLine("Records inserted into database");
        }
    }

    private bool TryParseRecord(string[] fields, int lineNumber, out TaxiTrip record)
    {
        record = new TaxiTrip();
        try
        {
            if (!DateTime.TryParseExact(fields[1], "MM/dd/yyyy hh:mm:ss tt", CultureInfo.InvariantCulture, 
                    DateTimeStyles.None, out var pickup))
            {
                _errors.Add($"Line {lineNumber}: Invalid pickup datetime format");
                return false;
            }

            if (!DateTime.TryParseExact(fields[2], "MM/dd/yyyy hh:mm:ss tt", CultureInfo.InvariantCulture, 
                    DateTimeStyles.None, out var dropoff))
            {
                _errors.Add($"Line {lineNumber}: Invalid dropoff datetime format");
                return false;
            }

            if (dropoff <= pickup)
            {
                _errors.Add($"Line {lineNumber}: Dropoff time is before or equal to pickup time");
                return false;
            }

            if ((dropoff - pickup).TotalHours > 24)
            {
                _errors.Add($"Line {lineNumber}: Trip duration is more than 24 hours");
                return false;
            }

            if (!short.TryParse(fields[3], out var passengerCount) || passengerCount <= 0)
            {
                _errors.Add($"Line {lineNumber}: Invalid passenger count");
                return false;
            }

            if (!decimal.TryParse(fields[4], out var tripDistance) || tripDistance < 0)
            {
                _errors.Add($"Line {lineNumber}: Invalid trip distance");
                return false;
            }

            if (!short.TryParse(fields[7], out var puLocationId))
            {
                _errors.Add($"Line {lineNumber}: Invalid PULocationID");
                return false;
            }

            if (!short.TryParse(fields[8], out var doLocationId))
            {
                _errors.Add($"Line {lineNumber}: Invalid DOLocationID");
                return false;
            }

            if (!decimal.TryParse(fields[10], out var fareAmount) || fareAmount < 0)
            {
                _errors.Add($"Line {lineNumber}: Invalid fare amount");
                return false;
            }

            if (!decimal.TryParse(fields[13], out var tipAmount) || tipAmount < 0)
            {
                _errors.Add($"Line {lineNumber}: Invalid tip amount");
                return false;
            }

            record = new TaxiTrip
            {
                PickupDateTime = TimeZoneInfo.ConvertTimeToUtc(pickup, _timeZone),
                DropoffDateTime = TimeZoneInfo.ConvertTimeToUtc(dropoff, _timeZone),
                PassengerCount = passengerCount,
                TripDistance = tripDistance,
                StoreAndForwardFlag = ConvertFlag(fields[6]),
                PULocationID = puLocationId,
                DOLocationID = doLocationId,
                FareAmount = fareAmount,
                TipAmount = tipAmount
            };

            return true;
        }
        catch (Exception ex)
        {
            _errors.Add($"Line {lineNumber}: Unexpected error - {ex.Message}");
            return false;
        }
    }

    private static string ConvertFlag(string flag) => flag?.ToUpper() switch
    {
        "Y" => "Yes",
        "N" => "No",
        _ => throw new ArgumentException($"Invalid store_and_fwd_flag value: {flag}")
    };

    private async Task WriteRecordsToCSV(IEnumerable<TaxiTrip> records, string filename)
    {
        var lines = new List<string>
        {
            // Header
            "PickupDateTime,DropoffDateTime,PassengerCount,TripDistance,StoreAndForwardFlag,PULocationID,DOLocationID,FareAmount,TipAmount"
        };

        // Data
        lines.AddRange(records.Select(r => 
            $"{r.PickupDateTime},{r.DropoffDateTime},{r.PassengerCount},{r.TripDistance}," +
            $"{r.StoreAndForwardFlag},{r.PULocationID},{r.DOLocationID},{r.FareAmount},{r.TipAmount}"));

        await File.WriteAllLinesAsync(filename, lines);
    }

    private async Task BulkInsertRecordsAsync(IEnumerable<TaxiTrip> records)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var bulkCopy = new SqlBulkCopy(connection)
        {
            DestinationTableName = "TaxiTrips",
            BatchSize = 10000,
            BulkCopyTimeout = 0
        };

        ConfigureBulkCopyMappings(bulkCopy);
        
        var dataTable = CreateDataTable(records);
        await bulkCopy.WriteToServerAsync(dataTable);
    }

    private static void ConfigureBulkCopyMappings(SqlBulkCopy bulkCopy)
    {
        var mappings = new[]
        {
            "PickupDateTime",
            "DropoffDateTime",
            "PassengerCount",
            "TripDistance",
            "StoreAndForwardFlag",
            "PULocationID",
            "DOLocationID",
            "FareAmount",
            "TipAmount"
        };

        foreach (var mapping in mappings)
        {
            bulkCopy.ColumnMappings.Add(mapping, mapping);
        }
    }

    private static DataTable CreateDataTable(IEnumerable<TaxiTrip> records)
    {
        var table = new DataTable();
        
        table.Columns.AddRange(new[]
        {
            new DataColumn("PickupDateTime", typeof(DateTime)),
            new DataColumn("DropoffDateTime", typeof(DateTime)),
            new DataColumn("PassengerCount", typeof(short)),
            new DataColumn("TripDistance", typeof(decimal)),
            new DataColumn("StoreAndForwardFlag", typeof(string)),
            new DataColumn("PULocationID", typeof(short)),
            new DataColumn("DOLocationID", typeof(short)),
            new DataColumn("FareAmount", typeof(decimal)),
            new DataColumn("TipAmount", typeof(decimal))
        });

        foreach (var record in records)
        {
            table.Rows.Add(
                record.PickupDateTime,
                record.DropoffDateTime,
                record.PassengerCount,
                record.TripDistance,
                record.StoreAndForwardFlag,
                record.PULocationID,
                record.DOLocationID,
                record.FareAmount,
                record.TipAmount
            );
        }

        return table;
    }
}

public record TaxiTrip
{
    public DateTime PickupDateTime { get; init; }
    public DateTime DropoffDateTime { get; init; }
    public short PassengerCount { get; init; }
    public decimal TripDistance { get; init; }
    public string StoreAndForwardFlag { get; init; } = "";
    public short PULocationID { get; init; }
    public short DOLocationID { get; init; }
    public decimal FareAmount { get; init; }
    public decimal TipAmount { get; init; }
}

public sealed class TaxiTripComparer : IEqualityComparer<TaxiTrip>
{
    public bool Equals(TaxiTrip? x, TaxiTrip? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x is null || y is null) return false;
        
        return x.PickupDateTime == y.PickupDateTime &&
               x.DropoffDateTime == y.DropoffDateTime &&
               x.PassengerCount == y.PassengerCount;
    }

    public int GetHashCode(TaxiTrip obj) =>
        HashCode.Combine(obj.PickupDateTime, obj.DropoffDateTime, obj.PassengerCount);
}