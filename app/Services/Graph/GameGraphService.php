<?php

declare(strict_types=1);

namespace App\Services\Graph;

use Illuminate\Support\Facades\File;
use PDO;

class GameGraphService
{
    private ?PDO $db = null;

    private string $dbPath;

    public function __construct()
    {
        $this->dbPath = storage_path('app/game_graph.sqlite');
    }

    private function initDatabase(): void
    {
        if ($this->db !== null) {
            return;
        }

        $exists = File::exists($this->dbPath);

        if (! $exists) {
            File::ensureDirectoryExists(dirname($this->dbPath));
            touch($this->dbPath);
        }

        $this->db = new PDO("sqlite:{$this->dbPath}");
        $this->db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        if (! $exists) {
            $this->db->exec('
                CREATE TABLE nodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    type TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    label TEXT,
                    prices JSON,
                    UNIQUE(type, external_id)
                );

                CREATE TABLE edges (
                    from_node_id INTEGER NOT NULL,
                    to_node_id INTEGER NOT NULL,
                    type TEXT NOT NULL,
                    weight REAL DEFAULT 1.0,
                    PRIMARY KEY (from_node_id, to_node_id, type),
                    FOREIGN KEY (from_node_id) REFERENCES nodes(id),
                    FOREIGN KEY (to_node_id) REFERENCES nodes(id)
                );

                CREATE INDEX idx_nodes_ext ON nodes(external_id);
                CREATE INDEX idx_edges_from ON edges(from_node_id);
                CREATE INDEX idx_edges_to ON edges(to_node_id);
            ');
        }
    }

    public function addNode(string $type, string $externalId, ?string $label = null, ?array $prices = null): int
    {
        $this->initDatabase();
        $stmt = $this->db->prepare('
            INSERT INTO nodes (type, external_id, label, prices) 
            VALUES (?, ?, ?, ?) 
            ON CONFLICT(type, external_id) DO UPDATE SET 
                label = COALESCE(excluded.label, nodes.label),
                prices = COALESCE(excluded.prices, nodes.prices)
            RETURNING id
        ');
        $stmt->execute([$type, $externalId, $label, $prices ? json_encode($prices) : null]);

        return (int) $stmt->fetchColumn();
    }

    public function addEdge(int $fromId, int $toId, string $type, float $weight = 1.0): void
    {
        $this->initDatabase();
        $stmt = $this->db->prepare('
            INSERT INTO edges (from_node_id, to_node_id, type, weight) 
            VALUES (?, ?, ?, ?)
            ON CONFLICT DO UPDATE SET weight = excluded.weight
        ');
        $stmt->execute([$fromId, $toId, $type, $weight]);
    }

    /**
     * Record a relationship for later processing (used by parallel workers).
     */
    public function recordRelationship(string $fromType, string $fromId, string $toType, string $toId, string $relType, ?string $fromLabel = null, ?string $toLabel = null): void
    {
        $logFile = storage_path('app/graph_queue_'.getmypid().'.jsonl');
        $data = json_encode([
            'from_type' => $fromType,
            'from_id' => $fromId,
            'from_label' => $fromLabel,
            'to_type' => $toType,
            'to_id' => $toId,
            'to_label' => $toLabel,
            'rel_type' => $relType,
        ]);

        file_put_contents($logFile, $data."\n", FILE_APPEND | LOCK_EX);
    }

    /**
     * Process all queued relationship files into the main graph.
     */
    public function processQueuedRelationships(): void
    {
        $this->initDatabase();
        $files = File::glob(storage_path('app/graph_queue_*.jsonl'));
        if (empty($files)) {
            return;
        }

        $this->beginTransaction();
        try {
            foreach ($files as $file) {
                $handle = fopen($file, 'r');
                while (($line = fgets($handle)) !== false) {
                    $data = json_decode($line, true);
                    if (! $data) {
                        continue;
                    }

                    $fromNodeId = $this->addNode($data['from_type'], (string) $data['from_id'], $data['from_label']);
                    $toNodeId = $this->addNode($data['to_type'], (string) $data['to_id'], $data['to_label']);
                    $this->addEdge($fromNodeId, $toNodeId, $data['rel_type']);
                }
                fclose($handle);
                File::delete($file);
            }
            $this->commit();
        } catch (\Throwable $e) {
            $this->rollback();
            throw $e;
        }
    }

    public function beginTransaction(): void
    {
        $this->initDatabase();
        $this->db->beginTransaction();
    }

    public function commit(): void
    {
        $this->initDatabase();
        $this->db->commit();
    }

    public function rollback(): void
    {
        if ($this->db && $this->db->inTransaction()) {
            $this->db->rollBack();
        }
    }
}
