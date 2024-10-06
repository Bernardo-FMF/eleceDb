package org.elece.db.page;

import org.elece.exception.db.DbException;

public interface PageFactory {
    Page getPage(PageTitle pageTitle) throws DbException;
}
