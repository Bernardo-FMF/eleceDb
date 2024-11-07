package org.elece.db.page;

import org.elece.exception.DbException;

public interface PageFactory {
    Page getPage(PageTitle pageTitle) throws DbException;
}
