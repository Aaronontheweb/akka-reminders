namespace Akka.Reminders.Sql.Configuration;

/// <summary>
/// Configuration settings for SQL-based reminder storage.
/// Controls database connection, schema, and initialization behavior.
/// </summary>
public sealed record SqlReminderStorageSettings
{
    /// <summary>
    /// The database connection string.
    /// </summary>
    public required string ConnectionString { get; init; }

    /// <summary>
    /// The database provider name.
    /// Supported values: "SqlServer", "PostgreSql"
    /// </summary>
    public required string ProviderName { get; init; }

    /// <summary>
    /// The schema name for the reminders table.
    /// Default: "reminders"
    /// </summary>
    public string SchemaName { get; init; } = "reminders";

    /// <summary>
    /// The table name for storing reminders.
    /// Default: "scheduled_reminders"
    /// </summary>
    public string TableName { get; init; } = "scheduled_reminders";

    /// <summary>
    /// Whether to automatically create the schema and table if they don't exist.
    /// Default: true (similar to Akka.Persistence behavior)
    /// </summary>
    public bool AutoInitialize { get; init; } = true;

    /// <summary>
    /// The timeout for database operations.
    /// Default: 30 seconds
    /// </summary>
    public TimeSpan CommandTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Creates settings for SQL Server.
    /// </summary>
    public static SqlReminderStorageSettings CreateSqlServer(
        string connectionString,
        string? schemaName = null,
        string? tableName = null,
        bool? autoInitialize = null)
    {
        return new SqlReminderStorageSettings
        {
            ConnectionString = connectionString,
            ProviderName = "SqlServer",
            SchemaName = schemaName ?? "reminders",
            TableName = tableName ?? "scheduled_reminders",
            AutoInitialize = autoInitialize ?? true
        };
    }

    /// <summary>
    /// Creates settings for PostgreSQL.
    /// </summary>
    public static SqlReminderStorageSettings CreatePostgreSql(
        string connectionString,
        string? schemaName = null,
        string? tableName = null,
        bool? autoInitialize = null)
    {
        return new SqlReminderStorageSettings
        {
            ConnectionString = connectionString,
            ProviderName = "PostgreSql",
            SchemaName = schemaName ?? "reminders",
            TableName = tableName ?? "scheduled_reminders",
            AutoInitialize = autoInitialize ?? true
        };
    }

    /// <summary>
    /// Validates the settings and throws if invalid.
    /// </summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new ArgumentException("ConnectionString cannot be null or empty.", nameof(ConnectionString));

        if (string.IsNullOrWhiteSpace(ProviderName))
            throw new ArgumentException("ProviderName cannot be null or empty.", nameof(ProviderName));

        if (ProviderName != "SqlServer" && ProviderName != "PostgreSql")
            throw new ArgumentException($"Unsupported provider: {ProviderName}. Supported providers: SqlServer, PostgreSql", nameof(ProviderName));

        if (string.IsNullOrWhiteSpace(SchemaName))
            throw new ArgumentException("SchemaName cannot be null or empty.", nameof(SchemaName));

        if (string.IsNullOrWhiteSpace(TableName))
            throw new ArgumentException("TableName cannot be null or empty.", nameof(TableName));

        if (CommandTimeout <= TimeSpan.Zero)
            throw new ArgumentException("CommandTimeout must be positive.", nameof(CommandTimeout));
    }
}
