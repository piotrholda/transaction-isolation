package com.piotrholda.transaction.isolation;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
interface AccountRepository extends JpaRepository<Account, Integer> {

    @Query(value = "SELECT BALANCE FROM ACCOUNT WHERE ID = :accountId", nativeQuery = true)
    int getBalance(int accountId);

    @Modifying
    @Query(value = "UPDATE ACCOUNT SET BALANCE = :balance WHERE ID = :accountId", nativeQuery = true)
    void updateBalance(int accountId, int balance);
}
