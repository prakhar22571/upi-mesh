package com.demo.upimesh.service;

import com.demo.upimesh.crypto.HybridCryptoService;
import com.demo.upimesh.model.MeshPacket;
import com.demo.upimesh.model.PaymentInstruction;
import com.demo.upimesh.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Orchestrates the full server-side pipeline for one inbound packet from a
 * bridge node:
 *
 *   1. Hash the ciphertext.
 *   2. Try to claim that hash via the idempotency cache.
 *      - If already claimed: this is a duplicate. Drop it.
 *   3. Decrypt the ciphertext with the server's private key.
 *      - If decryption fails: tampered or junk. Reject.
 *   4. Check freshness -- reject if signedAt is too old (replay protection).
 *   5. Hand off to SettlementService for the actual debit/credit.
 */
@Service
public class BridgeIngestionService {

    private static final Logger log = LoggerFactory.getLogger(BridgeIngestionService.class);

    @Autowired private HybridCryptoService crypto;
    @Autowired private IdempotencyService idempotency;
    @Autowired private SettlementService settlement;

    @Value("${upi.mesh.packet-max-age-seconds:86400}")
    private long maxAgeSeconds;

    private final AtomicInteger settledCount   = new AtomicInteger();
    private final AtomicInteger rejectedCount  = new AtomicInteger();
    private final AtomicInteger duplicateCount = new AtomicInteger();
    private final AtomicInteger invalidCount   = new AtomicInteger();

    public Map<String, Integer> getStats() {
        return Map.of(
            "settled",        settledCount.get(),
            "rejected",       rejectedCount.get(),
            "duplicateDropped", duplicateCount.get(),
            "invalid",        invalidCount.get()
        );
    }

    public void resetStats() {
        settledCount.set(0);
        rejectedCount.set(0);
        duplicateCount.set(0);
        invalidCount.set(0);
    }

    public IngestResult ingest(MeshPacket packet, String bridgeNodeId, int hopCount) {
        String packetHash = "?";
        try {
            packetHash = crypto.hashCiphertext(packet.getCiphertext());

            // ---- Idempotency gate ----
            if (!idempotency.claim(packetHash)) {
                log.info("DUPLICATE packet {} from bridge {} - dropped",
                        packetHash.substring(0, 12) + "...", bridgeNodeId);
                duplicateCount.incrementAndGet();
                return IngestResult.duplicate(packetHash);
            }

            // ---- Decrypt ----
            PaymentInstruction instruction;
            try {
                instruction = crypto.decrypt(packet.getCiphertext());
            } catch (Exception e) {
                log.warn("Decryption failed for packet {}: {}",
                        packetHash.substring(0, 12) + "...", e.getMessage());
                invalidCount.incrementAndGet();
                return IngestResult.invalid(packetHash, "decryption_failed");
            }

            // ---- Freshness check (replay protection) ----
            long ageSeconds = (Instant.now().toEpochMilli() - instruction.getSignedAt()) / 1000;
            if (ageSeconds > maxAgeSeconds) {
                log.warn("Packet {} too old ({}s), rejected",
                        packetHash.substring(0, 12) + "...", ageSeconds);
                invalidCount.incrementAndGet();
                return IngestResult.invalid(packetHash, "stale_packet");
            }
            if (ageSeconds < -300) {
                invalidCount.incrementAndGet();
                return IngestResult.invalid(packetHash, "future_dated");
            }

            // ---- Settle ----
            Transaction tx = settlement.settle(instruction, packetHash, bridgeNodeId, hopCount);
            if (tx.getStatus() == Transaction.Status.SETTLED) {
                settledCount.incrementAndGet();
            } else {
                rejectedCount.incrementAndGet();
            }
            return IngestResult.fromTransaction(packetHash, tx);

        } catch (IllegalArgumentException e) {
            log.warn("Invalid payment data in packet {}: {}", packetHash, e.getMessage());
            invalidCount.incrementAndGet();
            return IngestResult.invalid(packetHash, "invalid_payment");
        } catch (Exception e) {
            log.error("Unexpected ingestion error: {}", e.getMessage(), e);
            invalidCount.incrementAndGet();
            return IngestResult.invalid("?", "internal_error");
        }
    }

    public record IngestResult(String outcome, String packetHash, String reason, Long transactionId) {
        public static IngestResult fromTransaction(String hash, Transaction tx) {
            return new IngestResult(tx.getStatus().name(), hash, null, tx.getId());
        }
        public static IngestResult duplicate(String hash) {
            return new IngestResult("DUPLICATE_DROPPED", hash, null, null);
        }
        public static IngestResult invalid(String hash, String reason) {
            return new IngestResult("INVALID", hash, reason, null);
        }
    }
}
