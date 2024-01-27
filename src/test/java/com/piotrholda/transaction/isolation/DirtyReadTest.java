package com.piotrholda.transaction.isolation;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.transaction.TransactionDefinition.*;

@Slf4j
@SpringBootTest
@Testcontainers
@ContextConfiguration(initializers = DirtyReadTest.Initializer.class)
class DirtyReadTest {

    @Container
    public static MSSQLServerContainer<?> sqlServerContainer =
            new MSSQLServerContainer<>(DockerImageName.parse("mcr.microsoft.com/mssql/server:latest"))
                    .acceptLicense();

    static class Initializer
            implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues.of(
                    "spring.datasource.url=" + sqlServerContainer.getJdbcUrl(),
                    "spring.datasource.username=" + sqlServerContainer.getUsername(),
                    "spring.datasource.password=" + sqlServerContainer.getPassword()
            ).applyTo(configurableApplicationContext.getEnvironment());
        }
    }

    @Autowired
    private PlatformTransactionManager platformTransactionManager;

    private TransactionTemplate transactionTemplate;

    @Autowired
    private AccountRepository accountRepository;

    @BeforeEach
    private void setUp() {
        transactionTemplate = new TransactionTemplate(platformTransactionManager);
    }

    public Account createAccount() {
        Account account = new Account();
        account.setBalance(1);
        log.info("Main thread: Create account with balance = 1");
        return accountRepository.save(account);
    }

    public void spendAndRollback(int accountId, int isolationLevel) {
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        def.setName("Transaction 1");
        def.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        def.setIsolationLevel(isolationLevel);
        TransactionStatus status = platformTransactionManager.getTransaction(def);
        Account account = accountRepository.getByIdNative(accountId);
        int balance = account.getBalance();
        log.info("Thread 1: Read balance = {}", balance);
        balance = balance - 1;
        log.info("Thread 1: Save balance = {}", balance);
        accountRepository.updateNative(accountId, balance);
        sleep(200);
        log.info("Thread 1: Rollback transaction 1.");
        platformTransactionManager.rollback(status);
    }

    public void spend(int accountId, int isolationLevel) {
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        def.setName("Transaction 2");
        def.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        def.setIsolationLevel(isolationLevel);
        TransactionStatus status = platformTransactionManager.getTransaction(def);
        sleep(100);
        Account account = accountRepository.getByIdNative(accountId);
        int balance = account.getBalance();
        log.info("Thread 2: Read balance = {}", balance);
        balance = balance - 1;
        log.info("Thread 2: Save balance = {}", balance);
        accountRepository.updateNative(accountId, balance);
        log.info("Thread 2: Commit transaction 2.");
        platformTransactionManager.commit(status);
    }

    private int executeTransactions(int isolationLevel) {
        Account account = createAccount();
        int accountId = account.getId();
        new Thread(() -> spendAndRollback(accountId, isolationLevel)).start();
        new Thread(() -> spend(accountId, isolationLevel)).start();
        sleep(400);
        int balance = accountRepository.getByIdNative(accountId).getBalance();
        log.info("Main thread: Read balance = {}", balance);
        return balance;
    }

    @Test
    void shouldReadDirtyStateWhenReadUncommited() {
        int balance = executeTransactions(ISOLATION_READ_UNCOMMITTED);
        assertThat(balance).isEqualTo(-1);
    }

    @Test
    void shouldNotReadDirtyStateWhenDefault() {
        int balance = executeTransactions(ISOLATION_DEFAULT);
        assertThat(balance).isEqualTo(0);
    }

    @Test
    void shouldNotReadDirtyStateWhenReadCommited() {
        int balance = executeTransactions(ISOLATION_READ_COMMITTED);
        assertThat(balance).isEqualTo(0);
    }

    @Test
    void shouldNotReadDirtyStateWhenRepeatableRead() {
        int balance = executeTransactions(ISOLATION_REPEATABLE_READ);
        assertThat(balance).isEqualTo(0);
    }

    @Test
    void shouldNotReadDirtyStateWhenSerializable() {
        int balance = executeTransactions(ISOLATION_SERIALIZABLE);
        assertThat(balance).isEqualTo(0);
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
